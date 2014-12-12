<?php
use DoSomething\MBStatTracker\StatHat;
use DoSomething\MB_Toolbox\MB_Toolbox;
use DoSomething\MB_Toolbox\MB_MailChimp;

/**
 * MBC_EMail_Subscription class - collect entries in the ?? queue to subscribe email addresses to an email service.
 */
class MBC_EMail_Subscription
{

  /**
   * Access credentials settings
   *
   * @var object
   */
  private $credentials;

  /**
   * Service settings
   *
   * @var array
   */
  private $settings;

  /**
   * Details of the channel connection in use by RabbitMQ.
   *
   * @var object
   */
  private $channel;

  /**
   * Message Broker object that details the connection to RabbitMQ.
   *
   * @var object
   */
  private $messageBroker;

  /**
   * Setting from external services - Message Broker Toolbox.
   *
   * @var object
   */
  private $toolbox;

  /**
   * Setting from external services - MailChimp.
   *
   * @var object
   */
  private $mailChimp;

  /**
   * Setting from external services - StatHat.
   *
   * @var object
   */
  private $statHat;

  /**
   * The number of queue entries to process in each session
   */
  const BATCH_SIZE = 100;

  /**
   * Constructor for MBC_EMail_Subscriptionp
   *
   * @param array $credentials
   *   Secret settings from mb-secure-config.inc
   *
   * @param array $config
   *   Configuration settings that define the Message Broker configuration
   *   including target exchanges and related querues.
   *
   * @param array $settings
   *   Settings from external services - Mailchimp
   */
  public function __construct($credentials, $config, $settings) {

    $this->credentials = $credentials;
    $this->config = $config;
    $this->settings = $settings;

    $this->messageBroker = new MessageBroker($credentials, $config);
    $connection = $this->messageBroker->connection;
    $this->channel = $connection->channel();

    $this->toolbox = new MB_Toolbox($settings);
    $this->mailChimp = new MB_MailChimp($settings);

    $this->statHat = new StatHat($settings['stathat_ez_key'], 'mbc-mailchimp-toolbox:');
    $this->statHat->setIsProduction(FALSE);
  }

  /* 
   * Consumer entries in mailchimpSubscriptionQueue
   *
   * @param string $payload
   *   The contents of the message in a serial format
   */
  public function consumeQueue() {
    echo '------- mbc-mailchimp-subscription->consumeQueue() START: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;

    // Get the status details of the queue by requesting a declare
    list($this->channel, $status) = $this->messageBroker->setupQueue($this->config['queue']['mailchimpSubscriptionQueue']['name'], $this->channel);

    $messageDetails = '';
    $newSubscribers = array();
    $processedCount = 0;

    $messageCount = $status[1];
    $unackedCount = $status[2];

    while ($messageCount > 0 && $processedCount < self::BATCH_SIZE) {
      $messageDetails = $this->channel->basic_get($this->config['queue']['mailchimpSubscriptionQueue']['name']);
      $messagePayload = unserialize($messageDetails->body);
      $newSubscribers[$processedCount] = array(
        'email' => $messagePayload['email'],
        'mb_delivery_tag' => $messageDetails->delivery_info['delivery_tag'],
      );

      if (isset($messagePayload['activity_timestamp'])) {
        $newSubscribers[$processedCount]['activity_timestamp'] = $messagePayload['activity_timestamp'];
      }
      if (isset($messagePayload['merge_vars']['FNAME'])) {
        $newSubscribers[$processedCount]['fname'] = $messagePayload['merge_vars']['FNAME'];
      }
      elseif (isset($messagePayload['first_name'])) {
        $newSubscribers[$processedCount]['fname'] = $messagePayload['first_name'];
      }
      if (isset($messagePayload['uid'])) {
        $newSubscribers[$processedCount]['uid'] = $messagePayload['uid'];
      }
      if (isset($messagePayload['birthdate_timestamp'])) {
        $newSubscribers[$processedCount]['birthdate_timestamp'] = $messagePayload['birthdate_timestamp'];
      }
      elseif (isset($messagePayload['birthdate'])) {
        $newSubscribers[$processedCount]['birthdate'] = $messagePayload['birthdate'];
      }
      if (isset($messagePayload['mobile'])) {
        $newSubscribers[$processedCount]['mobile'] = $messagePayload['mobile'];
      }
      if (isset($messagePayload['activity'])) {
        $newSubscribers[$processedCount]['activity'] = $messagePayload['activity'];
      }
      if (isset($messagePayload['country_code'])) {
        $newSubscribers[$processedCount]['country_code'] = $messagePayload['country_code'];
      }

      // CGG2014 specific
      if (isset($messagePayload['candidate_name'])) {
        $newSubscribers[$processedCount]['cgg2014_vote_candidate_name'] = $messagePayload['candidate_name'];
      }

      // MailChimp settings
      if (isset($messagePayload['mailchimp_grouping_id'])) {
        $newSubscribers[$processedCount]['mailchimp_grouping_id'] = $messagePayload['mailchimp_grouping_id'];
      }
      if (isset($messagePayload['mailchimp_group_name'])) {
        $newSubscribers[$processedCount]['mailchimp_group_name'] = $messagePayload['mailchimp_group_name'];
      }

      $messageCount--;
      $processedCount++;
    }

    list($composedSubscriberList, $mbDeliveryTags) = $this->composeSubscriberSubmission($newSubscribers);
    if (count($composedSubscriberList) > 0) {
      $subscriptionResults = $this->mailChimp->submitBatchToMailChimp($composedSubscriberList);
      $this->ackBackSubscribers($subscriptionResults, $mbDeliveryTags);

      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('consumeQueue');
      $this->statHat->reportCount($processedCount);
    }
    else {
      echo 'consumeQueue() - No new accounts to submit to MailChimp.', PHP_EOL;
    }

    echo '------- mbc-mailchimp-subscription->consumeQueue() END: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;
  }

  /**
   * Format email list to meet MailChimp API requirements for batchSubscribe
   *
   * @param array $newSubscribers
   *   The list of email address to be formatted
   *
   * @return array
   *   Array of email addresses formatted to meet MailChimp API requirements.
   */
  private function composeSubscriberSubmission($newSubscribers = array()) {

    $composedSubscriberList = array();
    $mbDeliveryTags = array();

    $composedSubscriberList = array();
    foreach ($newSubscribers as $newSubscriberCount => $newSubscriber) {

      // Consolidate varable names
      // @todo: Convert all producers to use first_name and last_name
      if (isset($newSubscriber['fname'])) {
        $newSubscriber['FNAME'] = $newSubscriber['fname'];
      }
      elseif (isset($newSubscriber['first_name'])) {
        $newSubscriber['FNAME'] = $newSubscriber['first_name'];
      }
      if (isset($newSubscriber['lname'])) {
        $newSubscriber['LNAME'] = $newSubscriber['lname'];
      }
      elseif (isset($newSubscriber['last_name'])) {
        $newSubscriber['LNAME'] = $newSubscriber['last_name'];
      }

      // Remove invalid values
      if (isset($newSubscriber['birthdate_timestamp']) && !is_int($newSubscriber['birthdate_timestamp']) ||
          $newSubscriber['birthdate_timestamp'] > (time() - (60 * 60 * 24 * 365 * 1))) {
        unset($newSubscriber['birthdate_timestamp']);
      }
      if (isset($newSubscriber['mobile']) && !strlen($newSubscriber['mobile']) < 8) {
        unset($newSubscriber['mobile']);
      }

      // Dont add address of users under 13
      if (!isset($newSubscriber['birthdate_timestamp']) ||
          (isset($newSubscriber['birthdate_timestamp']) &&
          ($newSubscriber['birthdate_timestamp'] < time() - (60 * 60 * 24 * 365 * 13)))) {

        if (isset($newSubscriber['country_code'])) {
          $countryCode = $newSubscriber['country_code'];
          if ($this->toolbox->isDSAffiliate($newSubscriber['country_code'])) {
            $groupings = array(
              array(
                'id' => 10669,
                'groups' => array($countryCode)
              ),
            );
          }
          else {
            $groupings = array(
              array(
                'id' => 10673,
                'groups' => array('Non-affiliates')
              ),
            );
          }
        }

        $composedSubscriberList[$newSubscriberCount] = array(
          'email' => array(
            'email' => $newSubscriber['email']
          ),
          'merge_vars' => array(
            'UID' => isset($newSubscriber['uid']) ? $newSubscriber['uid'] : '',
            'FNAME' => isset($newSubscriber['FNAME']) ? $newSubscriber['FNAME'] : '',
            'LNAME' => isset($newSubscriber['LNAME']) ? $newSubscriber['LNAME'] : '',
            'BDAY' => isset($newSubscriber['birthdate_timestamp']) ? date('m/d', $newSubscriber['birthdate_timestamp']) : '',
            'BDAYFULL' => isset($newSubscriber['birthdate_timestamp']) ? date('m/d/Y', $newSubscriber['birthdate_timestamp']) : '',
            'MOBILE' => isset($newSubscriber['mobile']) ? $newSubscriber['mobile'] : '',
            'C_CODE' => isset($newSubscriber['country_code']) ? $newSubscriber['country_code'] : '',
            'SOURCE' => isset($newSubscriber['activity']) ? $newSubscriber['activity'] : '',
            'groupings' => $groupings
          ),
        );
        // CGG2014
        if (isset($newSubscriber['cgg2014_vote_candidate_name'])) {
          $composedSubscriberList[$newSubscriberCount]['merge_vars']['CGG14_VOTE'] = $newSubscriber['cgg2014_vote_candidate_name'];
        }
        
        // Index of email and related message ID for ack_back
        $mbDeliveryTags[$newSubscriber['email']] = $newSubscriber['mb_delivery_tag'];
      }
      else {
        $this->channel->basic_ack($newSubscriber['mb_delivery_tag']);

        $this->statHat->clearAddedStatNames();
        $this->statHat->addStatName('Under 13 - skipped');
        $this->statHat->reportCount(1);
      }
    }

    return array($composedSubscriberList, $mbDeliveryTags);
  }

  /**
   * Acknowledge user signup to MailChimp as batch. Remove entry from queue.
   *
   * @param array $subscriptionResults
   *   The list of email address to be formatted
   *
   * @return array
   *   Batch submission results from subscription submission to MailChimp.
   */
  private function ackBackSubscribers($subscriptionResults, $mbDeliveryTags) {
    
    

    $this->statHat->clearAddedStatNames();
    $this->statHat->addStatName('ackBackSubscribers - Add');
    foreach ($subscriptionResults['adds'] as $subscriptionResultAdds) {
      if (isset($mbDeliveryTags[$subscriptionResultAdds['email']])) {
        $this->channel->basic_ack($mbDeliveryTags[$subscriptionResultAdds['email']]);
        $this->statHat->reportCount(1);
      }
      else {
        echo 'ERROR - ackBackSubscribers Add: No delivery tag for: ' . $subscriptionResultAdds['email'], PHP_EOL;
        $this->statHat->clearAddedStatNames();
        $this->statHat->addStatName('ackBackSubscribers - Add ERROR');
        $this->statHat->reportCount(1);
      }
    }

    $this->statHat->clearAddedStatNames();
    $this->statHat->addStatName('ackBackSubscribers - Update');
    foreach ($subscriptionResults['updates'] as $subscriptionResultUpdates) {
      if (isset($mbDeliveryTags[$subscriptionResultUpdates['email']])) {
        $this->channel->basic_ack($mbDeliveryTags[$subscriptionResultUpdates['email']]);
        $this->statHat->reportCount(1);
      }
      else {
        echo 'ERROR - ackBackSubscribers Update: No delivery tag for: ' . $subscriptionResultUpdates['email'], PHP_EOL;
        $this->statHat->clearAddedStatNames();
        $this->statHat->addStatName('ackBackSubscribers - Update ERROR');
        $this->statHat->reportCount(1);
      }

    }

    $this->statHat->clearAddedStatNames();
    $this->statHat->addStatName('ackBackSubscribers - Errors');
    if (count($subscriptionResults['errors']) > 0) {
      $this->statHat->reportCount(1);
    }

  }

}
