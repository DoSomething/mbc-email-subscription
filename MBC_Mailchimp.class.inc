<?php

use DoSomething\MBStatTracker\StatHat;

/**
 * MBC_Mailchimp class - functionality related to the Message Broker use of the
 * Mailchimp service.
 */
class MBC_MailChimp
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
   * Setting from external services - StatHat.
   *
   * @var object
   */
  private $statHat;

  /**
   * Message Broker object that details the connection to RabbitMQ.
   *
   * @var object
   */
  private $messageBroker;

    /**
   * The number of queue entries to process in each session
   */
  const BATCH_SIZE = 100;

  /**
   * Constructor for MBC_Mailchimp
   *
   * @param array $credentials
   *   Secret settings from mb-secure-config.inc
   *
   * @param array $config
   *   Configuration settings from mb-config.inc
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

    $this->statHat = new StatHat($settings['stathat_ez_key'], 'mbc-mailchimp-toolbox:');
    $this->statHat->setIsProduction(FALSE);
  }

  /* 
   * Consumer entries in mailchimpSubscriptionQueue
   *
   * @param string $payload
   *   The contents of the message in a serial format
   */
  public function consumeMailchimpSubscriptionQueue() {
    echo '------- mbc-mailchimp-subscription->consumeQueue() START: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;

$bla = FALSE;
if ($bla) {
  $bla = TRUE;
}

   // Get the status details of the queue by requesting a declare
    list($this->channel, $status) = $this->messageBroker->setupQueue($this->config->topicEmailService->queues->mailchimpSubscriptionQueue->name, $this->channel);

    $messageCount = $status[1];
    $unackedCount = $status[2];

    $messageDetails = '';
    $newSubscribers = array();
    $processedCount = 0;

    while ($messageCount > 0 && $processedCount < self::BATCH_SIZE) {
      $messageDetails = $this->channel->basic_get($this->config->topicEmailService->queues->mailchimpSubscriptionQueue->name);
      $messagePayload = unserialize($messageDetails->body);
      $newSubscribers[$processedCount] = array(
        'email' => $messagePayload['email'],
        'mb_delivery_tag' => $messageDetails->delivery_info['delivery_tag'],
      );
      if (isset($messagePayload['merge_vars']['FNAME'])) {
        $newSubscribers[$processedCount]['fname'] = $messagePayload['merge_vars']['FNAME'];
      }
      if (isset($messagePayload['uid'])) {
        $newSubscribers[$processedCount]['uid'] = $messagePayload['uid'];
      }
      if (isset($messagePayload['birthdate_timestamp'])) {
        $newSubscribers[$processedCount]['birthdate_timestamp'] = $messagePayload['birthdate_timestamp'];
      }
      elseif (isset($messagePayload['birthdate'])) {
        $newSubscribers[$processedCount]['birthdate_timestamp'] = $messagePayload['birthdate'];
      }
      if (isset($messagePayload['mobile'])) {
        $newSubscribers[$processedCount]['mobile'] = $messagePayload['mobile'];
      }

      $messageCount--;
      $processedCount++;
    }

    list($composedSubscriberList, $mbDeliveryTags) = $this->composeSubscriberSubmission($newSubscribers);
    if (count($composedSubscriberList) > 0) {
      $results = $this->submitToMailChimp($composedSubscriberList, $mbDeliveryTags);
      $this->statHat->clearAddedStatNames();
      $this->statHat->addStatName('consumeNewRegistrationsQueue');
      $this->statHat->reportCount($processedCount);
    }
    else {
      $results = 'No new accounts to submit to MailChimp.';
    }

    return $results;

    
    


    echo '------- mbc-mailchimp-subscription->consumeQueue() END: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;
  }
  
  
  /**
   * Make signup submission to MailChimp
   *
   * @param array $composedBatch
   *   The list of email address to be submitted to MailChimp
   *
   * @param array $mbDeliveryTags
   *   A list of RabbitMQ delivery tags being processed in batch.
   *
   * @return array
   *   A list of the RabbitMQ queue entry IDs that have been successfully
   *   submitted to MailChimp.
   */
  private function submitBatchToMailChimp($composedBatch = array(), $mbDeliveryTags = array()) {

    // Debugging
    // $results1 = $MailChimp->call("lists/list", array());
    // $results2 = $MailChimp->call("lists/interest-groupings", array('id' => 'f2fab1dfd4'));

    // batchSubscribe($id, $batch, $double_optin=true, $update_existing=false, $replace_interests=true)
    // replace_interests: optional - flag to determine whether we replace the
    // interest groups with the updated groups provided, or we add the provided
    // groups to the member's interest groups (optional, defaults to true)
        // Lookup list details including "mailchimp_list_id"
    // -> 71893 "Do Something Members" is f2fab1dfd4 (who knows why?!?)

    $results = $this->mailChimp->call("lists/batch-subscribe", array(
      'id' => $this->settings['mailchimp_int_list_id'],
      'batch' => $composedBatch,
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE
    ));

  }

  /**
   * Make single signup submission to MailChimp. Typically used for resubscribes.
   *
   * @param array $composedBatch
   *   The list of email address to be submitted to MailChimp
   *
   * @param array $mbDeliveryTags
   *   A list of RabbitMQ delivery tags being processed in batch.
   *
   * @return array
   *   A list of the RabbitMQ queue entry IDs that have been successfully
   *   submitted to MailChimp.
   */
  private function submitToMailChimp($email = '', $mbDeliveryTag = array()) {

    // Submit subscription to Mailchimp
    $mc = new \Drewm\MailChimp($this->settings['mailchimp_apikey']);

    $results = $mc->call("lists/subscribe", array(
      'id' => $this->settings['mailchimp_list_id'],
      'email' => array(
        'email' => $composedItem['email']['email']
        ),
      'merge_vars' => $composedItem['merge_vars'],
      'double_optin' => FALSE,
      'update_existing' => TRUE,
      'replace_interests' => FALSE,
      'send_welcome' => FALSE,
    ));

  }

}
