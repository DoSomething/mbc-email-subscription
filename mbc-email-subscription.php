<?php
/**
 * mbc-mailchimp-subscription.php
 *
 * Consume queue entries in mailchimpSubscriptionQueue to process messages
 * from external applications regarding users subscriptions to Mailchimp.
 */

// Load up the Composer autoload magic
require_once __DIR__ . '/vendor/autoload.php';

// Load configuration settings common to the Message Broker system
// symlinks in the project directory point to the actual location of the files
require_once __DIR__ . '/mb-secure-config.inc';

require_once __DIR__ . '/MBC_EMail_Subscription.class.inc';
require_once __DIR__ . '/messagebroker-config/MB_Configuration.class.inc';


// Settings
$credentials = array(
  'host' =>  getenv("RABBITMQ_HOST"),
  'port' => getenv("RABBITMQ_PORT"),
  'username' => getenv("RABBITMQ_USERNAME"),
  'password' => getenv("RABBITMQ_PASSWORD"),
  'vhost' => getenv("RABBITMQ_VHOST"),
);
$settings = array(
  'mailchimp_apikey' => getenv("MAILCHIMP_APIKEY"),
  'mailchimp_int_list_id' => getenv("MAILCHIMP_INTERNATIONAL_LIST_ID"),
  'stathat_ez_key' => getenv("STATHAT_EZKEY"),
);

$config = array();
$source = __DIR__ . '/messagebroker-config/mb_config.json';
$mb_config = new MB_Configuration($source, $settings);
$topicEmailService = $mb_config->exchangeSettings('topicEmailService');

$config['exchange'] = array(
  'name' => $topicEmailService->name,
  'type' => $topicEmailService->type,
  'passive' => $topicEmailService->passive,
  'durable' => $topicEmailService->durable,
  'auto_delete' => $topicEmailService->auto_delete,
);
$config['queue'][$topicEmailService->queues->mailchimpSubscriptionQueue->name] = array(
  'name' => $topicEmailService->queues->mailchimpSubscriptionQueue->name,
  'passive' => $topicEmailService->queues->mailchimpSubscriptionQueue->passive,
  'durable' => $topicEmailService->queues->mailchimpSubscriptionQueue->durable,
  'exclusive' => $topicEmailService->queues->mailchimpSubscriptionQueue->exclusive,
  'auto_delete' => $topicEmailService->queues->mailchimpSubscriptionQueue->auto_delete,
  'bindingKey' => $topicEmailService->queues->mailchimpSubscriptionQueue->binding_pattern,
);


echo '------- mbc-mailchimp-subscription START: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;

// Kick off
$status = '';
$mbcEMailSubscription = new MBC_EMail_Subscription($credentials, $config, $settings);
$status = $mbcEMailSubscription->consumeQueue();

print $status;

echo '------- mbc-mailchimp-subscription END: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;
