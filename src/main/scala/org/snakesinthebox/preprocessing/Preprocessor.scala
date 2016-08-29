package org.snakesinthebox.preprocessing

/**
  * Created by brad on 8/27/16.
  */

object Preprocessor extends PreprocessorFunctions{

  def removeNumbers(word: String): Boolean = {
    word.matches("[^0-9]*")
  }

  def removeSpecials(word: String): String = {
    word.replaceAll("&[a-zA-Z]*;", "")
  }

  def removeForwardSlash(word: String): String = {
    word.replaceAll("\\/", " ")
  }

  def removePunctuation(word: String): String = {
    word.replaceAll("\\p{Punct}", "")
  }
}