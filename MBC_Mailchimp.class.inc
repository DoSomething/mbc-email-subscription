<?php

use DoSomething\MBStatTracker\StatHat;

/**
 * MBC_Mailchimp class - functionality related to the Message Broker use of the
 * Mailchimp service.
 */
class MBC_Mailchimp
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
   * Setting from external services - StatHat.
   *
   * @var object
   */
  private $statHat;

  /**
   * Constructor for MBC_Mailchimp
   *
   * @param array $credentials
   *   Connection credentials
   * @param array $settings
   *   Settings of additional services used by the class.
   */
  public function __construct($credentials, $settings) {

    $this->credentials = $credentials;
    $this->settings = $settings;

    $this->statHat = new StatHat($settings['stathat_ez_key'], 'mbc-mailchimp-subscription:');
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

    $message = unserialize($payload->body);

    echo '------- mbc-mailchimp-subscription->consumeQueue() END: ' . date('D M j G:i:s T Y') . ' -------', PHP_EOL;
  }

}
