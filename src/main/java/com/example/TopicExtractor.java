package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.language.v1.LanguageServiceClient;

public class TopicExtractor {


  public static class ExtractTopics extends DoFn<String, ClassifyTextResponse>{
      @Override 
      public void processElement(ProcessContext c){
          // use Language service client here to get topics....
          try (LanguageServiceClient language = LanguageServiceClient.create()) {
                String text = c.element();
                // set content to the text string
                Document doc = Document.newBuilder()
                    .setContent(text)
                    .setType(Type.PLAIN_TEXT)
                    .build();
                ClassifyTextRequest request = ClassifyTextRequest.newBuilder()
                    .setDocument(doc)
                    .build();
                c.output(request);
             }
        }


      /** A DoFn that converts a Word and Count into a printable string. */
      // modify this for ClassifyTextResponse objects to readable strings....
  public static class FormatAsTextFn extends DoFn<ClassifyTextResponse, String> {
    @Override
    public void processElement(ProcessContext c) {
      //c.output(c.element().getKey() + ": " + c.element().getValue());

      ClassifyTextResponse response = c.element();

      String topics = "";
      for (ClassificationCategory category : response.getCategoriesList()) {
        topics += "Topic name :"+ category.getName() +"Confidence :"+
         category.getConfidence() + "\n";
      }

      c.output(topics);
    }
  }

     /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * a list of topics.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

    public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);

    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile())) // reads each line and converts to string;set input file to scraper output file
     .apply(ParDo.of(new ExtractTopics()))
     .apply(ParDo.of(new FormatAsTextFn())) // formatting responses into actual strings
     .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

    p.run();
  }
}