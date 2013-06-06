require 'ffi/aspell'

class Suggester
  # Returns an array of suggested corrections for a query_string.
  #
  # Currently only returns a single suggestion, where each word:
  #  * is retained if in the dictionary, otherwise
  #  * is replaced by it's most likely correction
  def suggestions(query_string)
    suggested_string = query_string.split("\s").map do |word|
      suggested_word = suggestion_for_a_word(word)
      if suggested_word.nil?
        word
      # If the word is the same (ignoring differences in letter cases),
      # retain the user's letter cases.
      elsif suggested_word.downcase == word.downcase
        word
      else
        suggested_word
      end
    end.join(" ")
    [suggested_string]
  end

private
  def suggestion_for_a_word(word)
    speller.suggestions(word).first
  end

  def speller
    # Creating a new speller reads files off disk, so we want to do that as
    # little as possible. Therefore, memoize it as a singleton.
    @@speller ||= FFI::Aspell::Speller.new('en_GB')
  end
end
