<?php

use DoSomething\MBStatTracker\StatHat;

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

    $this->statHat = new StatHat($settings['stathat_ez_key'], 'mbc-mailchimp-toolbox:');
    $this->statHat->setIsProduction(FALSE);
  }

  /* 
   * Consumer entries in mailchimpSubscriptionQueue
   *
   * @param string $payload
   *   The contents of the message in a serial format
   */
  public function consumeQueue($payload) {
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

    echo '------- mbc-mailchimp-subscription->consumeQueue() END: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;
  }
  

}
