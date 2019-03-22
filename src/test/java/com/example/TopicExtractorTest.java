
package com.example;

import com.example.WordCount.CountWords;
import com.example.WordCount.ExtractWordsFn;
import com.example.WordCount.FormatAsTextFn;
import com.example.TopicExtractor.ExtractTopics;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class TopicExtractorTest {
    
      /** Example test that tests a specific DoFn. */
  @Test
  public void testExtractTopicsFn() {
    DoFnTester<String, ClassifyTextResponse> extractWordsFn =
        DoFnTester.of(new ExtractTopics());
    String input = "Welcome to a weekly collaboration between"+ "FiveThirtyEight and ABC News. With 5,000 people seemingly thinking about challenging President Trump"+ "in 2020 2014 Democrats and even some Republicans 2014 we 2019re"+" keeping tabs on the field as it develops."+ "Each week, we 2019ll run through what the potential candidates are up to 2014";// + "who 2019s getting closer to"+ "officially jumping in the ring";//+ "and who 2019s getting further away";
    Assert.assertThat(extractWordsFn.processBatch(input),
                      CoreMatchers.hasItems("some", "input", "words")); // change these to classify text response objects I guess...
    
    /*Assert.assertThat(extractWordsFn.processBatch(" "),
                      CoreMatchers.<String>hasItems());
    Assert.assertThat(extractWordsFn.processBatch(" some ", " input", " words"),
                      CoreMatchers.hasItems("some", "input", "words"));*/
  }

  
}