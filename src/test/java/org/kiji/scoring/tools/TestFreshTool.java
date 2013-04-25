package org.kiji.scoring.tools;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.mapreduce.tools.KijiProduce;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * Created with IntelliJ IDEA. User: aaron Date: 4/24/13 Time: 6:08 PM To change this template use
 * File | Settings | File Templates.
 */
public class TestFreshTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFreshTool.class);

  public static final class TestProducer extends KijiProducer {

    @Override
    public KijiDataRequest getDataRequest() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getOutputColumn() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  private String[] mToolOutputLines;

  private int runTool(BaseTool tool, String...arguments) throws Exception {
    mToolOutputBytes.reset();
    final PrintStream pstream = new PrintStream(mToolOutputBytes);
    tool.setPrintStream(pstream);
    tool.setConf(getConf());
    try {
      LOG.info("Running tool: '{}' with parameters {}", tool.getName(), arguments);
      return tool.toolMain(Lists.newArrayList(arguments));
    } finally {
      pstream.flush();
      pstream.close();

      mToolOutputStr = Bytes.toString(mToolOutputBytes.toByteArray());
      LOG.info("Captured output for tool: '{}' with parameters {}:\n{}\n{}{}\n",
          tool.getName(), arguments,
          RULER, mToolOutputStr, RULER);
      mToolOutputLines = mToolOutputStr.split("\n");
    }
  }

  //------------------------------------------------------------------------------------------------

  @Before
  public final void setupTestFreshTool() throws Exception {

  }

  @After
  public final void cleanupTestFreshTool() throws Exception {

  }

  @Test
  public void testRegister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
        "--do=register",
        "org.kiji.scoring.ShelfLife",
        "10",
        "org.kiji.scoring.tools.TestFreshTool$TestProducer"
    ));

    final KijiFreshnessPolicyRecord record = KijiFreshnessPolicyRecord.newBuilder()
        .setRecordVersion("policyrecord-0.1.0")
        .setProducerClass(TestProducer.class.getName())
        .setFreshnessPolicyClass(Class.forName("org.kiji.scoring.ShelfLife")
            .asSubclass(KijiFreshnessPolicy.class).getName())
        .setFreshnessPolicyState("10")
        .build();
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    assertEquals(record, manager.retrievePolicy("user", "info:name"));
  }
}
