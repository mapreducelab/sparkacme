package com.mapreducelab.spark.anomalies

import org.scalatest.FlatSpec

class AnomaliesSpec extends FlatSpec {

  "A isValidEmail function" should "validate email" in {

    println(s"test isValidEmail function with different input options: ${isValidEmail("balu@gmail.com")::isValidEmail("a@kuksin.us")::isValidEmail("a @kuksin.us")::isValidEmail("!@gmail.com")::isValidEmail("hotmail.com")::isValidEmail(" ")::isValidEmail("")::Nil}")

    assert(isValidEmail("balu@gmail.com") == true)

    assert(isValidEmail("a@kuksin.us") == true)

    assert(isValidEmail("a @kuksin.us") == false)

    assert(isValidEmail("!@gmail.com") == false)

    assert(isValidEmail("hotmail.com") == false)

    assert(isValidEmail(" ") == false)

    assert(isValidEmail("") == false)
  }

  "A isValidZipCode function" should "validate zip code with 5 digit length strictly based on the mysql schema." in {

    // validate zip code - the strict 5 digit lenght is based on the mysql schema zip5 column length - zip5 int(5).

    println(s"test isValidZipCode function with different input options: ${isValidZipCode("36066")::isValidZipCode("1066")::isValidZipCode("3606")::isValidZipCode("13606")::Nil}")

    assert(isValidZipCode("36066") == true)

    assert(isValidZipCode("1066") == false)

    assert(isValidZipCode("3606") == false)

    assert(isValidZipCode("13606") == true)

  }

  "A isValidPhoneNumber function" should "validate phone number without a country code." in {

    // Assuming that the database schema phone column length is 10 Int, this is for phone numbers without a country code.

    println(s"test isValidPhoneNumber function with different input options: ${ isValidPhoneNumber("36066")::isValidPhoneNumber("2394151757")::isValidPhoneNumber("239415175756")::Nil}")

    assert(isValidPhoneNumber("36066") == false)

    assert(isValidPhoneNumber("2394151757") == true)

    assert(isValidPhoneNumber("239415175756") == false)

  }

}
