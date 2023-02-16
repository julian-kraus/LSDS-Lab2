package edu.upf;
import java.io.IOException;

// tool that for a list of files read the tweet pass it if of interest got if not discard
public interface LanguageFilter {

  /**
   * Process
   * @param language
   * @return
   */
  void filterLanguage(String language) throws Exception;
}
