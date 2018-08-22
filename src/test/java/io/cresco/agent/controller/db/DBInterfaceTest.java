package io.cresco.agent.controller.db;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;

import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import com.jayway.jsonpath.JsonPath;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import io.cresco.agent.controller.globalscheduler.pNode;
import io.cresco.library.messaging.MsgEvent;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.json.simple.parser.*;

import io.cresco.agent.controller.db.testhelpers.*;


import static org.junit.jupiter.api.Assertions.*;


@org.junit.jupiter.api.TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DBInterfaceTest {

    private DBInterface gdb;
    private JSONParser jp;
    private final String NOT_FOUND = "This should return whatever we choose for \"not found\"";

    private final GDBConf testingConf = new GDBConf("test_runner","test","localhost","cresco_test");

    //TODO:The hardcoded paths below are bad news. I need to get them in here some other way
    private final String db_export_file_path = "/home/nima/code/IdeaProjects/cresco_db_rewrite/src/test/resources/global_region_agent.gz";
    private final String regional_db_export_file_path = "/home/nima/code/IdeaProjects/cresco_db_rewrite/src/test/resources/cresco_regional.gz";

    private ODatabaseDocumentTx model_db;
    private ODatabaseDocumentTx test_db;

    //Note: The following should match what's in the test database *exactly*
    //At this time the configs are set up so that the agent name will change on a subsequent invocation.
    //If I make changes to the test db, these need to be rechecked!
    private static final Set<String> agents = new HashSet<>(Arrays.asList("agent_smith","gc_agent","rc_agent","",null));
    private static final Set<String> regions = new HashSet<>(Arrays.asList("global_region","different_test_region","",null));
    private static final Set<String> pluginids = new HashSet<>(Arrays.asList("plugin/0","plugin/1","plugin/2","",null));
    private static final Set<String> jarfiles = new HashSet<>(Arrays.asList("repo-1.0-SNAPSHOT.jar","sysinfo-1.0-SNAPSHOT.jar","dashboard-1.0-SNAPSHOT.jar","",null));

    //The following is used in tests of several "resourceInfo" methods
    private static final List<String> categories = Arrays.asList(new String[]{"disk_available", "disk_total", "mem_available", "cpu_core_count", "mem_total"});
    Map<String,Map<String,Map<String,String>>> region_contents = new HashMap<>();

    private static final int REPEAT_COUNT = 5;

    Stream<Arguments> getRegionAgentPluginidTriples(){
        return regions.stream().flatMap( (region) ->
                agents.stream().flatMap( (agent) ->
                        pluginids.stream().map( (pluginid) ->
                                Arguments.of(region,agent,pluginid)
                        )
                )
        );
    }

    Stream<Arguments> getRegionAgentPairs(){
        return regions.stream().flatMap( (r)->
                agents.stream().map( (a) ->
                        Arguments.of(r,a)
                )
        );
    }

    Stream<String> getRegions(){
        return regions.stream();
    }

    Stream<String> getAgents(){
        return agents.stream();
    }

    Stream<String> getPluginIds(){
        return pluginids.stream();
    }

    Stream<String> getJarfiles(){
        return jarfiles.stream();
    }

    List<Arguments> getPluginTypeIdValuePairs(){
        /*Values pulled from test db using following query:
          select indexes.name
          from (select indexes from metadata:indexmanager unwind indexes)
          where indexes[name] like 'pNode%'
        */
        List<Arguments> ret  = new ArrayList<>();
        for(String pname : Arrays.asList(new String[]{"io.cresco.dashboard","io.cresco.sysinfo","io.cresco.repo","",null})){
            ret.add(Arguments.of("pluginname",pname));
        }

        getRegionAgentPluginidTriples().forEach( (argset) ->
                ret.add(Arguments.of("nodePath",String.format("[\"%s\",\"%s\",\"%s\"]",argset.get())))
        );
        for(String jar : jarfiles){
            ret.add(Arguments.of("jarfile",jar));
        }
        ret.add(Arguments.of(null,null));
        return ret;
    }

    Stream<String> getPipelineId(){
        return Stream.of("",null);
    }

    Stream<String> getResourceids(){
        return Stream.of("sysinfo_resource","netdiscovery_resource","container_resource","controllerinfo_resource","",null);
    }

    Stream<String> getInodeids(){
        return Stream.of("sysinfo_inode","netdiscovery_inode","container_inode","controllerinfo_inode","",null);
    }

    Stream<Boolean> getBooleans(){
        return Stream.of(true,false,null);
    }

    Stream<Arguments> getResourceidInodeidIsResourceMetricTriples(){
        return getResourceids().flatMap( (resource)->
                getInodeids().flatMap( (inode) ->
                        getBooleans().map( (bool) ->
                                Arguments.of(resource,inode,bool)
                        )
                )
        );
    }

    Stream<Arguments> getRegionAgentPluginidBooleanQuads(){
        return getRegionAgentPluginidTriples().flatMap( (rap_args)->
                getBooleans().map( (bools) ->
                        Arguments.of(rap_args,bools)
                )
        );

    }

    boolean regionExists(String region){
        return region != null && region_contents.containsKey(region);
    }

    boolean agentExists(String region, String agent){
        return agent != null && regionExists(region) && region_contents.get(region).containsKey(agent);
    }

    boolean pluginExist(String region, String agent, String plugin){
        return plugin!= null && agentExists(region,agent) && region_contents.get(region).get(agent).containsKey(plugin);
    }
    @org.junit.jupiter.api.BeforeAll
    void init_model_db() {
        Map<String,String> plugins_reg_agents = new HashMap<>();
        Map<String,Map<String,String>> agents_in_diff_region = new HashMap<>();
        plugins_reg_agents.put("plugin/0","io.cresco.sysinfo");
        agents_in_diff_region.put("agent_smith",plugins_reg_agents);
        agents_in_diff_region.put("rc_agent",plugins_reg_agents);
        region_contents.put("different_test_region",agents_in_diff_region);

        Map<String,String> plugins_gc = new HashMap<>();
        plugins_gc.put("plugin/0","io.cresco.repo");
        plugins_gc.put("plugin/1","io.cresco.dashboard");
        plugins_gc.put("plugin/2","io.cresco.sysinfo");
        Map<String,Map<String,String>> agents_in_global_region = new HashMap<>();
        agents_in_global_region.put("gc_agent",plugins_gc);
        region_contents.put("global_region",agents_in_global_region);

        jp = new JSONParser();
        try {
            model_db = OrientHelpers.getInMemoryTestDB(db_export_file_path).orElseThrow(() -> new Exception("Can't get test db"));
        }
        catch(Exception ex){
            System.out.println("Caught some exception while trying to set up DB for tests");
            ex.printStackTrace();
        }

    }
    @BeforeEach
    void set_up_controller() {
        Map<String,Object> pluginConf = CrescoHelpers.getMockPluginConfig(testingConf.getAsMap());
        test_db = model_db.copy();
        gdb = new DBInterface4Test(test_db);
    }

   /* @AfterEach
    void tear_down_controller(){
        ce.setDBManagerActive(false);
        test_db.close();
    }*/

    @RepeatedTest(REPEAT_COUNT)
    void paramStringToMap_test() {
        String testParams="param1=value1,param2=value2";
        Map<String,String> resultMap = gdb.paramStringToMap(testParams);
        assertEquals("value1",resultMap.get("param1"));
        assertEquals("value2",resultMap.get("param2"));
    }


    /**
     * It seems the test DB I created does not have any records for this function to return. It throws an exception
     * because of a null value (see in DBInterface.java in getResourceTotal() the call edgeParams.get("cpu-logical-count")
     * I need to find out why there are no records. Based on the code, there should at least be the keys I check for in
     * the map returned.
     */
    @Test
    void getResourceTotal_test() {
        Map<String,String> resourceTotals = gdb.getResourceTotal();
        assertNotNull(resourceTotals.get("regions"));
        assertNotNull(resourceTotals.get("agents"));
        assertNotNull(resourceTotals.get("plugins"));
        assertNotNull(resourceTotals.get("cpu_core_count"));
        assertNotNull(resourceTotals.get("mem_available"));
        assertNotNull(resourceTotals.get("mem_total"));
        assertNotNull(resourceTotals.get("disk_available"));
        assertNotNull(resourceTotals.get("disk_total"));
    }

    @RepeatedTest(REPEAT_COUNT)
    void getRegionList_test() {
        String desired_output = "{\"regions\":[{\"name\":\"different_test_region\",\"agents\":\"2\"},{\"name\":\"globa"+
                "l_region\",\"agents\":\"1\"}]}";
        assertEquals(desired_output, gdb.getRegionList());
    }

    /**
     * This may not be needed in the updated version of the api.  We will need *some* mechanism to share databases but
     * that may happen elsewhere. The part of the test code that finds and preps the import file will have to be changed
     *
     */
    @Test
    void submitDBImport_test() {
        String expected_regions = "{\"regions\":[{\"name\":\"different_test_region\",\"agents\":\"2\"},{\"name\":\""+
                "global_region\",\"agents\":\"1\"},{\"name\":\"yet_another_test_region\",\"agents\":\"1\"}]}";
        String expected_agents = "{\"agents\":[{\"environment\":\"environment\",\"plugins\":\"1\",\"name\":\"another_r"+
                "c\",\"location\":\"location\",\"region\":\"yet_another_test_region\",\"platform\":\"platform\"}]}";
        String expected_plugins = "{\"plugins\":[{\"agent\":\"another_rc\",\"name\":\"plugin/0\",\"region\":\"yet_anot"+
                "her_test_region\"}]}";
        try {
            byte[] importData = Files.readAllBytes(Paths.get(regional_db_export_file_path));
            gdb.submitDBImport(DatatypeConverter.printBase64Binary(importData));
            Thread.sleep(5000);//Import runs in another thread so we need to give it time to work
            String newRegionList = gdb.getRegionList();
            assertEquals(expected_regions,newRegionList);
            String newAgentList = gdb.getAgentList("yet_another_test_region");
            assertEquals(expected_agents,newAgentList);
            String newPluginList = gdb.getPluginList("yet_another_test_region","another_rc");
            assertEquals(expected_plugins,newPluginList);
        }
        catch(FileNotFoundException ex) {
            fail(String.format("Could not find regional db export file at %s",regional_db_export_file_path),ex);
        }
        catch(IOException ex) {
            fail(String.format("Could not read regional db export file at %s",regional_db_export_file_path),ex);
        }
        catch(InterruptedException ex) {
            System.out.println("Threadus Interruptus");
        }
    }

    /**
     * Calling this with a null argument returns a different result from a blank argument or one not in the db. Is that
     * really the desired behavior? This test was written so it would pass if the null, blank, and nonexistent cases all
     * produce the same result.
     * @param region
     */
    @ParameterizedTest
    @MethodSource("getRegions")
    void getAgentList_test(String region) {
        Map<String,String> expected_output = new HashMap<>();
        expected_output.put("global_region"
                ,"{\"agents\":[{\"environment\":\"environment\",\"plugins\":\"3\",\"name\":\"gc_agent\""+
                        ",\"location\":\"location\",\"region\":\"global_region\",\"platform\":\"platform\"}]}");
        expected_output.put("different_test_region"
                ,"{\"agents\":[{\"environment\":\"environment\",\"plugins\":\"1\",\"name\":\"agent_smith\""+
                        ",\"location\":\"location\",\"region\":\"different_test_region\",\""+
                        "platform\":\"platform\"},{\"environment\":\"environment\",\"plugins\":\"1\",\"name\":\"rc_agent\",\"l"+
                        "ocation\":\"location\",\"region\":\"different_test_region\",\"platform\":\"platform\"}]}");
        if(region == null || region.equals("")){
            assertEquals("{\"agents\":[]}",gdb.getAgentList(region));
        }
        assertEquals(expected_output.get(region), gdb.getAgentList(region));

    }

    /**
     * The function 'getPluginListRepoSet' only seems to use the database to figure out where the repo plugin lives (should be global
     * controller). Thus we may need a test that checks the lower-level function the aforementioned one depends on.
     */
    @RepeatedTest(REPEAT_COUNT)
    void getPluginListRepo_test() {
        assertEquals("{\"plugins\":[{\"jarfile\":\"fake.jar\",\"version\":\"NO_VERSION\",\"md5\":\"DefinitelyR"+
                "ealMD5\"}]}",gdb.getPluginListRepo());
    }

    /**
     * The function 'getPluginListRepoSet' only seems to use the database to figure out where the repo plugin lives (should be global
     * controller). Thus we may need a test that checks the lower-level function the aforementioned one depends on.
     */
    @RepeatedTest(REPEAT_COUNT)
    void getPluginListRepoSet_test() {
        Map<String,List<pNode>> plist = gdb.getPluginListRepoSet();
        for(pNode aplugin : plist.get("some_plugin_name")){
            assertTrue(
                    aplugin.isEqual("some_plugin_name","some_plugin.jar","65388b8d8bf462df2cd3910bcada4110","9.99.999")
            );
        }

    }

    /**
     * This test is similar to the getPluginListRepoSet_test() because it depends on the same lower-level db function.
     */
    @RepeatedTest(REPEAT_COUNT)
    void getPluginListRepoInventory_test() {
        String expected = "{\"server\":[],\"plugins\":[{\"pluginname\":\"some_plugin_name\",\"jarfile\":\"some_plugin."+
                "jar\",\"version\":\"9.99.999\",\"md5\":\"65388b8d8bf462df2cd3910bcada4110\"}]}";
        List<String> repoList = gdb.getPluginListRepoInventory();
        for(String res : repoList){
            assertEquals(expected,res);
        }
    }

    /**
     *I intentionally left the test for null args e.g. ("pluginname",null) failing. I think we should add
     * something in the new implementation to avoid the NulLPointerException.
     *We may not want this thing to work the same way after the rewrite. It seems odd that we should
     *get a different result just for null input. Also, this function will never return useful results for
     *this case as written because the query has a hardcoded part like this: '...WHERE key = '" + indexValue + "'".
     *The problem with that is that 'key' may be a collection instead of a single value when the index uses a composite
     *key. We could change the method or rename/annotate it to make it more clear what will work/not work.
     *If nothing else, it could be a good thing to mention in a docstring.
     * @param typeId
     * @param typeVal
     */
    @ParameterizedTest
    @MethodSource("getPluginTypeIdValuePairs")
    void getPluginListByType_test(String typeId,String typeVal) {
        String actual = gdb.getPluginListByType(typeId,typeVal);
        String sysinfo_expected = "{\"plugins\":[{\"agent\":\"gc_agent\",\"status_code\":\"10\",\"agentcon"+
                "troller\":\"plugin/2\",\"pluginname\":\"io.cresco.sysinfo\",\"status_dest\":\"Plugin "+
                "Active\",\"jarfile\":\"sysinfo-1.0-SNAPSHOT.jar\",\"pluginid\":\"plugin/2\",\"isactiv"+
                "e\":\"true\",\"region\":\"global_region\",\"configparams\":\"{\\\"pluginname\\\":\\\""+
                "io.cresco.sysinfo\\\",\\\"jarfile\\\":\\\"sysinfo-1.0-SNAPSHOT.jar\\\"}\"},{\"agent\""+
                ":\"agent_smith\",\"status_code\":\"10\",\"agentcontroller\":\"plugin/0\",\"pluginnam"+
                "e\":\"io.cresco.sysinfo\",\"status_dest\":\"Plugin Active\",\"jarfile\":\"sysinfo-1.0"+
                "-SNAPSHOT.jar\",\"pluginid\":\"plugin/0\",\"isactive\":\"true\",\"region\":\"differen"+
                "t_test_region\",\"configparams\":\"{\\\"pluginname\\\":\\\"io.cresco.sysinfo\\\",\\\""+
                "jarfile\\\":\\\"sysinfo-1.0-SNAPSHOT.jar\\\"}\"},{\"agent\":\"rc_agent\",\"status_cod"+
                "e\":\"10\",\"agentcontroller\":\"plugin/0\",\"pluginname\":\"io.cresco.sysinfo\",\"st"+
                "atus_dest\":\"Plugin Active\",\"jarfile\":\"sysinfo-1.0-SNAPSHOT.jar\",\"pluginid\":"+
                "\"plugin/0\",\"isactive\":\"true\",\"region\":\"different_test_region\",\"configparam"+
                "s\":\"{\\\"pluginname\\\":\\\"io.cresco.sysinfo\\\",\\\"jarfile\\\":\\\"sysinfo-1.0-S"+
                "NAPSHOT.jar\\\"}\"}]}";
        String dashboard_expected = "{\"plugins\":[{\"agent\":\"gc_agent\",\"status_code\":\"10\",\"agentcon"+
                "troller\":\"plugin/1\",\"pluginname\":\"io.cresco.dashboard\",\"status_dest\":\"Plugi"+
                "n Active\",\"jarfile\":\"dashboard-1.0-SNAPSHOT.jar\",\"pluginid\":\"plugin/1\",\"isa"+
                "ctive\":\"true\",\"region\":\"global_region\",\"configparams\":\"{\\\"pluginname\\\":"+
                "\\\"io.cresco.dashboard\\\",\\\"jarfile\\\":\\\"dashboard-1.0-SNAPSHOT.jar\\\"}\"}]}";

        String repo_expected  = "{\"plugins\":[{\"agent\":\"gc_agent\",\"status_code\":\"10\",\"agentcon"+
                "troller\":\"plugin/0\",\"pluginname\":\"io.cresco.repo\",\"status_dest\":\"Pl"+
                "ugin Active\",\"jarfile\":\"repo-1.0-SNAPSHOT.jar\",\"pluginid\":\"plugin/0\""+
                ",\"isactive\":\"true\",\"region\":\"global_region\",\"configparams\":\"{\\\"pluginnam"+
                "e\\\":\\\"io.cresco.repo\\\",\\\"jarfile\\\":\\\"repo-1.0-SNAPSHOT.jar\\\"}\"}]}";

        switch(typeId) {
            case "pluginname":
                switch(typeVal){
                    case "io.cresco.dashboard": assertEquals(dashboard_expected,actual);break;
                    case "io.cresco.sysinfo": assertEquals(sysinfo_expected,actual);break;
                    case "io.cresco.repo": assertEquals(repo_expected,actual);
                }
                break;

            case "nodePath":
                assertEquals("{\"plugins\":[]}",actual);
                break;

            case "jarfile":
                switch(typeVal){
                    case "repo-1.0-SNAPSHOT.jar": assertEquals(repo_expected,actual); break;
                    case "sysinfo-1.0-SNAPSHOT.jar":assertEquals(sysinfo_expected,actual); break;
                    case "dashboard-1.0-SNAPSHOT.jar":assertEquals(dashboard_expected,actual); break;
                }
                break;
            default: assertEquals("{\"plugins\":[]}",actual);
        }
    }

    /**
     * Yep, some of these tests were written to fail for the previous implementation. Like many other functions in here,
     *  behavior changes when null args are passed vs blank strings. For two inputs where each can be one of: 1)in the
     *  database, 2)not in the database, or 3)null. That gives us nine total combinations. For cases out of those nine
     *  where we don't expect output, I suspect the "no output" response should be the same. Like several other functions,
     *  it can either be {"plugins":[]} or the test throws a NullPointerException. NPEs are undesirable because they
     *  don't tell you much. If we want to throw an exception in those cases, we should throw a more descriptive exception.
     * @param region
     * @param agent
     * @throws ParseException
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPairs")
    void getPluginList_test(String region, String agent) throws ParseException {
        String actual = gdb.getPluginList(region,agent);
        String allplugins = "{\"plugins\":[{\"agent\":\"agent_smith\",\"name\":\"plugin/0\",\"region\":\"different_test_region\"},{\"agent\":\"rc_agent\",\"name\":\"plugin/0\",\"region\":\"different_test_region\"},{\"agent\":\"gc_agent\",\"name\":\"plugin/0\",\"region\":\"global_region\"},{\"agent\":\"gc_agent\",\"name\":\"plugin/1\",\"region\":\"global_region\"},{\"agent\":\"gc_agent\",\"name\":\"plugin/2\",\"region\":\"global_region\"}]}";
        String global_region_plugins = "{\"plugins\":[{\"agent\":\"gc_agent\",\"name\":\"plugin/0\",\"region\":\"global_region\"},{\"agent\":\"gc_agent\",\"name\":\"plugin/1\",\"region\":\"global_region\"},{\"agent\":\"gc_agent\",\"name\":\"plugin/2\",\"region\":\"global_region\"}]}";
        String diff_region_plugins = "{\"plugins\":[{\"agent\":\"agent_smith\",\"name\":\"plugin/0\",\"region\":\"different_test_region\"},{\"agent\":\"rc_agent\",\"name\":\"plugin/0\",\"region\":\"different_test_region\"}]}";

        if(region == null && agent == null) assertEquals(allplugins,actual);
        else if(region.equals("global_region") && agent == null) assertEquals(global_region_plugins,actual);
        else if(region.equals("different_test_region") && agent == null) assertEquals(diff_region_plugins,actual);
        else if(region_contents.containsKey(region) && region_contents.get(region).containsKey(agent)){
            for (String pluginid : region_contents.get(region).get(agent).keySet()) {
                assertEquals(String.format("{\"plugins\":[{\"agent\":\"%s\",\"name\":\"%s\",\"region\":\"%s\"}]}"
                        ,agent,pluginid,region)
                        ,actual);
            }
        } else assertEquals("{\"plugins\":[]}",actual);
    }

    /**
     * This method does not seem to return aggregated results when some parameters are left null. In other words,
     * it doesn't look like passing a null argument does anything here. This differs from the examples I've seen so far.
     * This time I decided to force the tests to fail explicitly. I wrote them to call fail() to remind myself that we
     * need to pick a standard behavior for all of these.
     * @param region
     * @param agent
     * @param pluginid
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPluginidTriples")
    void getPluginInfo_test(String region, String agent, String pluginid) {
        String actual = gdb.getPluginInfo(region,agent,pluginid);
        if(region_contents.containsKey(region) && region_contents.get(region).containsKey(agent)){
            if(region.equals("global_region")){
                //Test db only has single agent in region "global_region". I already check the agent above so I don't
                //need to check it again. If we get here, the "agent" parameter could only have been a valid one.
                switch(pluginid){
                    case "plugin/0":assertEquals("{\"pluginname\":\"io.cresco.repo\",\"jarfile\":\"repo-1.0-SNAPSHOT.jar\"}",actual);
                        break;
                    case "plugin/1":assertEquals("{\"pluginname\":\"io.cresco.dashboard\",\"jarfile\":\"dashboard-1.0-SNAPSHOT.jar\"}",actual);
                        break;
                    case "plugin/2":assertEquals("{\"pluginname\":\"io.cresco.sysinfo\",\"jarfile\":\"sysinfo-1.0-SNAPSHOT.jar\"}",actual);
                        break;
                    default:fail(NOT_FOUND);
                }
            }
            if(region.equals("different_test_region")){
                //Two agents in this region but each only has one plugin,io.cresco.sysinfo.
                if(pluginid.equals("plugin/0")) assertEquals("{\"pluginname\":\"io.cresco.sysinfo\",\"jarfile\":\"sysinfo-1.0-SNAPSHOT.jar\"}"
                        ,actual);
                else fail(NOT_FOUND);
            }
        }
        else fail(NOT_FOUND);

    }

    /**
     * This never returns any results but seems like it should given the inputs. This is similar to
     * {@link #getResourceTotal_test()}.
     * @param region
     * @param agent
     * @param pluginid
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPluginidTriples")
    void getIsAttachedMetrics_test(String region, String agent, String pluginid) {
        String actual = gdb.getIsAttachedMetrics(region, agent, pluginid);
        assertNotEquals("[]",actual);
    }

    /**
     * This returns null. See {@link #getIsAttachedMetrics_test(String, String, String)}
     * and {@link #getResourceTotal_test()}
     */
    @Test
    void getNetResourceInfo_test() {
        String actual = gdb.getNetResourceInfo();
        assertNotNull(actual);
    }

    /**
     * getResourceInfo() does different things if one or both args is null. Private methods in DBInterface are called
     * depending on how many arguments are null. Also, the output of this function is a Base64 encoded string. We may
     * want to consider using different types for encoded+compressed string data so it's clear that regular String ops
     * might not work by themselves. We could perhaps pull some methods into that class from other places like
     * stringCompress from DBBaseFunctions.
     * @param region
     * @param agent
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPairs")
    void getResourceInfo_test(String region, String agent) {
        String actual = gdb.getResourceInfo(region, agent);
        System.out.println(actual);
        if((region != null) && (agent != null)) {
            assertEquals(gdb.getAgentResourceInfo(region, agent),actual);
        } else if (region != null && (agent == null)) {
            assertEquals(gdb.getRegionResourceInfo(region),actual);
        } else if (region == null && agent != null) {
            fail("Need to decide what \"not found\" should look like");
        } else {
            assertEquals(gdb.getGlobalResourceInfo(),actual);
        }
    }

    /**
     * I had to do some trickery to parse the JSON returned by this function. Things below the level of 'perf' in the
     * returned JSON object were coming back as a single string. This caused JSONPath to throw a ClassCastException.
     * @param region
     * @param agent
     * @throws ParseException
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPairs")
    void getAgentResourceInfo_test(String region, String agent) {
        String actual = gdb.gdb.stringUncompress(gdb.getAgentResourceInfo(region, agent));
        System.out.println(actual);
        List<String>perf_categories = Arrays.asList(new String[]{"disk","os","mem","part","cpu","net","fs"});
        //boolean regionExists = region != null && region_contents.containsKey(region);
        //boolean agentExists = region != null && agent != null && region_contents.get(region).containsKey(agent);
        if(regionExists(region) && agentExists(region,agent)){
            //NMS the following trickery is necessary to get a clean JSON object back. For whatever reason perfs_str
            //really is a string.
            String perfs_str = JsonPath.read(actual,"$.agentresourceinfo[0].perf");
            Map<String,List<Map<String,String>>> perfs = JsonPath.read(perfs_str,"$");
            //NMS The values for these counters will be different if I create a new dataset, and perhaps different ones
            //will be available depending on which system. Thus, I only check for the existence of certain keys.
            for(String cat : perf_categories){
                assertTrue(perfs.containsKey(cat));
                for(Map<String,String> perf_obj : perfs.get(cat)){
                    for(String key:perf_obj.keySet()){
                        //System.out.println(key);
                        assertNotNull(perf_obj.get(key));
                    }
                }
            }
        } else {
            fail(NOT_FOUND);
        }
    }

    /**
     * This test does a little more thorough check on the results compared to getAgentResourceInfo_test() since
     * there aren't as many values to sort through and they're all numeric. Returns a compressed+encoded string.
     * @param region
     */
    @ParameterizedTest
    @MethodSource("getRegions")
    void getRegionResourceInfo_test(String region) {
        String actual = gdb.gdb.stringUncompress(gdb.getRegionResourceInfo(region));
        System.out.println(actual);
        //boolean regionExists = region != null && region_contents.containsKey(region);
        if(regionExists(region)) {
            Map<String, String> region_resource_info = JsonPath.read(actual, "$.regionresourceinfo[0]");
            for (String category : categories) {
                assertTrue(Long.valueOf(region_resource_info.get(category)) > 0L);
            }
        } else {
            fail(NOT_FOUND);
        }
    }
    /**
     * This one is similar to the case for regions and agents. Returns a compressed+encoded string!
     */
    @RepeatedTest(REPEAT_COUNT)
    void getGlobalResourceInfo_test(){
        String actual = gdb.gdb.stringUncompress(gdb.getGlobalResourceInfo());
        System.out.println(actual);
        Map<String,String> global_resource_info = JsonPath.read(actual,"$.globalresourceinfo[0]");
        for(String category : categories){
            assertTrue(Long.valueOf(global_resource_info.get(category)) > 0L );
        }
    }


    /**
     * There are no pipeline records in test db so this does little else besides
     * check behavior when the input is not found or null.
     * @param pipelineid
     */
    @ParameterizedTest
    @MethodSource("getPipelineId")
    void getGPipeline_test(String pipelineid) {
        String actual = gdb.getGPipeline(pipelineid);
        System.out.println(actual);
        fail(NOT_FOUND);
    }

    /**
     * Returns compressed+encoded string!
     * @param pipelineid
     */
    @ParameterizedTest
    @MethodSource("getPipelineId")
    void getGPipelineExport(String pipelineid) {
        String actual = gdb.gdb.stringUncompress(gdb.getGPipelineExport(pipelineid));
        System.out.println(actual);
        fail(NOT_FOUND);
    }


    /**
     * This method returns a compressed+encoded string.
     * Please remember that this is not a good test. It is only here to check that
     * whatever changes we make will still give us the original output, which might
     * not even be desirable if the changes are big.
     *
     * @param resourceid
     * @param inodeid
     * @param isResourceMetric
     */
    @ParameterizedTest
    @MethodSource("getResourceidInodeidIsResourceMetricTriples")
    void getIsAssignedInfo_test(String resourceid, String inodeid, boolean isResourceMetric) {
        String actual = gdb.gdb.stringUncompress(gdb.getIsAssignedInfo(resourceid, inodeid, isResourceMetric));
        String expected = "{\"routepath-agent_smith\":\"17391\",\"agent\":\"agent_smith\"," +
                "\"agentcontroller\":\"plugin/0\"," +
                "\"resource_id\":\"sysinfo_resource\"," +
                "\"routepath-gc_agent\":\"29039\"," +
                "\"inode_id\":\"sysinfo_inode\"," +
                "\"region\":\"different_test_region\"," +
                "\"perf\":\"H4sIAAAAAAAAAN1VUW/bNhD+K4KfWiCySYqkRD9ty+ZuQDMUW" +
                "/NQtHmgJdohIokCRTl1i/z33TGS4yT1kL7GNmD77nh33933Ud9nle1vZsvP3" +
                "+OP9NbbYNb7YPrZclZQkskiIzmbnR250cUYK2g2mXv7zaBRSCKJoDwjfHI1rjI1+C7bm9bdtpPZG11NZUTGcpqpgpPJ2+oG8y0qs1u0u8aQlk6u4HXbb4wPNoYUWa7kcVJMSDMismJ2d3YaFCO8eAYqKyT7ASjOcyZpDg3+DCgmSS5o8TC7Y1BVk5ITiBgcoc8hsYJD/R9DIs+QkCcgKGEZp4Tm8icgUKokyU71f2oj/GnrXMzurs5mro8s6/d92uh22OgyDN54CHj39+XivW2Hr3Cy8640fZ+WbmgDTgNseGToxvRKFYUajRvd2HoPxl/rzrYmmZKgz8WFzos5SdaDrauEz6mYkzQj6da0xtsydtWYJrYF387v0+CCxqlQKQtZEK5UbOBWdw8uHCXQvCCTZ+hNNc58TKN32tZ6XWPDOeVSiJwDu7Fgp32IFfHHtB4BjMyJolgs2psR/vR/nP2ohY5Odls9kQm4kCPHyZkQAuTKUZ0vzM9O52cRRdkNEQR8p7Xb2lLXh5UhAdDeD02jPa7nrzaY+s0/b5Nz582bjxdvE5un+adcJOcfLpNfEjrPyLs/v43nWlsaSKrjTOdktHbX+/5RGTaVgWWP0WKOmra95GuLIcEPZoyCHfkpjMkYh2Zb1YdaMp/zg9nEGrFxyZNVJFoikwtUTUI5S/4NpgPSbRN1OINEXJ3T1W+rFQF6EPmHisNqzbTx8saEPu3vkysep3xv86Y0dhd5lGXosFDag0jMtBsTrnEWtW1B1Z2JoRRuMZFLAi8kny5xZGzJ2ZLoJSHxE5N1O5nqqvIgrSjtz1cYHwbMIeJhCOGPQ75A+jm++ZcZhhvvnQdhDWEk+2iw7fg/3hsTOg4yfTAewcsFF3jHop77oJsu9pBlikqqlFD3t8Xm/rbYuXqI6N3O+Frv4dx0DeE1Y/rSW7gZHLZwgcRIPjiYHPgOEoRhwRRx7URxJeGpJSSuP+y7x4mjwB+iueJEsEyx+HyZBINlhyHuOgrt0GAUSaO7zvjFsIbgIU1329Q7F46a/t+gx3DeO2B78js+nV+OxnwN/MVQ9NbEUb0WPCaUC6Cvq3fz0rWbV4bs2vUhNvf6YPXHmK7u/gOureT1lAoAAA\\u003d\\u003d\",\"routepath-rc_agent\":\"20975\"}";
        if(resourceid.equals("sysinfo_resource")
                && inodeid.equals("sysinfo_inode")
                && !isResourceMetric){
            assertEquals(expected,actual);
        } else {
            fail(NOT_FOUND);
        }
    }



    /**
     * This method returns a compressed+encoded string. There are no pipeline
     * records in the testdb, so this test only checks for
     * problems with null or "not found" args. The application-level stuff
     * will probably live on in another place, so I will defer writing proper
     * tests for that stuff until I'm working on that part.
     * @param pipeline_action
     */
    @ParameterizedTest
    @MethodSource("getPipelineId")
    void getPipelineInfo_test(String pipeline_action) {
        String actual = gdb.gdb.stringUncompress(gdb.getPipelineInfo(pipeline_action));
        System.out.println(actual);
        fail(NOT_FOUND);
    }

    /**
     * This method returns something closely coupled with the underlying database (record IDs).
     * In the implementation, plugin is only checked for null or not null, so all non-null values
     * for argument "plugin" should be interchangable. Perhaps the method signature should change?
     * Also, there are only two calls to this method in the controller package.
     * Perhaps it could be simplified? A better test should be written after changing the public API.
     * @param region
     * @param agent
     * @param plugin
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPluginidTriples")
    void getEdgeHealthStatus_test(String region, String agent, String plugin) {
        Map<String, NodeStatusType> actual = gdb.getEdgeHealthStatus(region, agent, plugin);
        System.out.println(actual.toString());
        //boolean regionExists = region != null && region_contents.containsKey(region);
        //boolean agentExists = agent != null && region_contents.get(region).containsKey(agent);
        if(regionExists(region) && agentExists(region,agent) && plugin == null){
            assertFalse(actual.isEmpty());
        }
        else if(regionExists(region) && agent == null && plugin == null){
            assertFalse(actual.isEmpty());
        }
        else if (region == null && agent == null && plugin == null){
            assertFalse(actual.isEmpty());
        }
        else {assertTrue(actual.isEmpty());}
    }

    /**
     * This test is exactly the same as {@link getEdgeHealthStatus_test(String, String, String)}
     * It should be replaced by a better, more specific test.
     * @param region
     * @param agent
     * @param plugin
     */
    @ParameterizedTest
    @MethodSource("getRegionAgentPluginidTriples")
    void getNodeStatus_test(String region, String agent, String plugin) {
        Map<String, NodeStatusType> actual = gdb.getNodeStatus(region, agent, plugin);
        System.out.println(actual.toString());
        if(regionExists(region) && agentExists(region,agent) && plugin == null){
            assertFalse(actual.isEmpty());
        }
        else if(regionExists(region) && agent == null && plugin == null){
            assertFalse(actual.isEmpty());
        }
        else if (region == null && agent == null && plugin == null){
            assertFalse(actual.isEmpty());
        }
        else {assertTrue(actual.isEmpty());}
    }

    MsgEvent getMsgEvent(String region, String agent, String plugin){
        MsgEvent de = new MsgEvent();
        de.setParams(new HashMap<>());
        if(region != null){de.setParam("region_name",region);}
        if(agent != null){de.setParam("agent_name",agent);}
        if(plugin != null){de.setParam("plugin_id",plugin);}
        return de;
    }

    Stream<MsgEvent4Test> getWatchDogMsgEvent() {
        return regions.stream().flatMap( (region) ->
                agents.stream().flatMap( (agent) ->
                        pluginids.stream().map( (plugin) ->
                                new MsgEvent4Test(getMsgEvent(region, agent, plugin))
                        )
                )
        );
    }

    void printMsgEventParams(MsgEvent m){
        m.getParams().keySet().stream().forEach((param) ->
                System.out.println(param+":"+m.getParam(param))
        );
    }

    /**
     * Most of the magic happens in the lower-level classes. The test only checks
     * that execution makes it through DBInterface.watchDogUpdate and not anything
     * in the implementation.
     * @param m
     */
    @ParameterizedTest
    @MethodSource("getWatchDogMsgEvent")
    void watchDogUpdate_test(MsgEvent4Test m){
        MsgEvent de = m.getMsgEvent();
        String region = de.getParam("region_name");
        String agent = de.getParam("agent_name");
        String pluginid = de.getParam("plugin_id");

        Boolean upd = gdb.watchDogUpdate(de);

        if(pluginExist(region,agent,pluginid)) {
            assertTrue(upd);
        } else if(agentExists(region, agent) && pluginid == null){
            assertTrue(upd);
        } else if(regionExists(region) && agent == null && pluginid == null){
            assertTrue(upd);
        } else {assertFalse(upd);}
    }


    /**
     * This method appears to be tightly coupled to the underlying database.
     * It may not be relevant after the rewrite.
     */
    @Test
    void addNode_test() {
        fail("To be removed");
    }

    /**
     * This method appears to be tightly coupled to the underlying database.
     * It may not be relevant after the rewrite.
     */
    @Test
    void removeNode_test() {
        fail("To be removed");
    }

    /**
     * This method appears to be tightly coupled to the underlying database.
     * It may not be relevant after the rewrite.
     */
    @Test
    void removeNode1_test() {
        fail("To be removed");
    }

}

