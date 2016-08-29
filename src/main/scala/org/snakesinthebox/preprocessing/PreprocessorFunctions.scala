package org.snakesinthebox.preprocessing

/**
  * Created by brad on 8/27/16.
  */

/**
  * All functions related to preprocessing of data
  */
trait PreprocessorFunctions {

  /**
    * Function to be used on filters to remove all numbers and
    * strings with numbers
    *
    * @param word instance of string from filter
    * @return
    */
  def removeNumbers(word: String): Boolean

  /**
    * Removes the substring of &[a-z-A-Z]*;
    *
    * @param word instance of string from map
    * @return
    */
  def removeSpecials(word: String): String

  /**
    * Removes any forward slash and replaces it with a space
    *
    * @param word instance of string from map
    * @return
    */
  def removeForwardSlash(word: String): String

  /**
    * Removes punctuation from string
    *
    * @param word instance of string from map
    * @return
    */
  def removePunctuation(word: String): String

}
