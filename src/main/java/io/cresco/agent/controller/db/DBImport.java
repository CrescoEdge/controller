package io.cresco.agent.controller.db;

/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
//package com.orientechnologies.orient.core.db.tool;

/*

Several functions modified from ODatabaseImport.java
https://github.com/orientechnologies/orientdb/blob/master/core/src/main/java/com/orientechnologies/orient/core/db/tool/ODatabaseImport.java

 */

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONReader;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import io.cresco.agent.controller.core.ControllerEngine;
import io.cresco.library.utilities.CLogger;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Import data from a file into a database.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */

public class DBImport {

    private OJSONReader jsonReader;
    private ORecord record;
    private Map<String, String>        convertedClassNames             = new HashMap<String, String>();

    private String importRegion;
    private List<String> importedList;

    //private Launcher agentcontroller;
    private CLogger logger;

    private ODatabaseDocumentTx db;
    private DBBaseFunctions gdb;

    public DBImport(ControllerEngine controllerEngine, final InputStream iStream, DBBaseFunctions gdb, ODatabaseDocumentTx db) {

        this.logger = controllerEngine.getPluginBuilder().getLogger(DBImport.class.getName(),CLogger.Level.Info);

        //this.logger = new CLogger(DBImport.class, agentcontroller.getMsgOutQueue(), agentcontroller.getRegion(), agentcontroller.getAgent(), agentcontroller.getPluginID(), CLogger.Level.Info);
        this.db = db;
        jsonReader = new OJSONReader(new InputStreamReader(iStream));
        this.gdb = gdb;
        importedList = new ArrayList<>();

        ODatabaseRecordThreadLocal.INSTANCE.set(db);
    }

    public boolean importDump() {

        try {

            long time = System.currentTimeMillis();
            jsonReader.readNext(OJSONReader.BEGIN_OBJECT);

            String tag;
            boolean clustersImported = false;
            while (jsonReader.hasNext() && jsonReader.lastChar() != '}') {
                tag = jsonReader.readString(OJSONReader.FIELD_ASSIGNMENT);

                if (tag.equals("records"))
                    importRecords();

            }
            cleanRecords();

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return true;
    }

    private void cleanRecords() {
        try {
            logger.debug("Cleaning Region :" + importRegion);

            List<String> regionList = gdb.getNodeIds(importRegion,null,null,false);
            logger.debug("Region " + importRegion + " has " + regionList.size() + " nodes ");
            for(String lostNode : regionList) {
                logger.debug("known nodes : " + lostNode);
            }

                for(String importedNode : importedList) {
                logger.debug("imported node " + importedNode);
                    regionList.remove(importedNode);
            }
            for(String lostNode : regionList) {
                logger.debug("Removing node :" + lostNode);
                Map<String,String> nodeParams = gdb.getNodeParams(lostNode);
                String region = nodeParams.get("region");
                String agent = nodeParams.get("agent");
                String pluginId = nodeParams.get("agentcontroller");
                logger.debug("Removing " + region + " " + agent + " " + pluginId);
                gdb.removeNode(region,agent,pluginId);
            }
            regionList.clear();
            importedList.clear();

        }
        catch(Exception ex) {
            logger.error(ex.toString());

        }

    }

    private long importRecords() throws Exception {
        jsonReader.readNext(OJSONReader.BEGIN_COLLECTION);

        long totalRecords = 0;

        logger.debug("\n\nImporting records...");

        final long begin = System.currentTimeMillis();
        long last = begin;



        //ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
        //ODatabaseRecordThreadLocal.INSTANCE.set((ODatabaseRecord) this.connection.getUnderlying().getUnderlying());

        while (jsonReader.lastChar() != ']') {


            ORecord rids = importRecord();
            rids.detach();

            logger.debug("Import Record : " + totalRecords);

            if (record instanceof ODocument) {

                ODocument document = (ODocument) record;
                document.detach();

                logger.debug("Cast to Document : " + totalRecords);


                //START


                //logger.error("ClassName: " + document.getClassName());
                Map<String,String> params = new HashMap<String,String>();

                for(String fieldName : document.fieldNames()) {
                    //logger.debug("Record Field : " + fieldName + " : " + document.field(fieldName));
                    if(!((fieldName.startsWith("in_") || (fieldName.startsWith("out_"))))) {
                        params.put(fieldName, document.field(fieldName).toString());
                    }
                }

                logger.debug("Generated " + params.size() + " params for :" + totalRecords);

                boolean isNodeType = false;
                String region = null;
                String agent = null;
                String plugin = null;
                if((document.containsField("region")) && (!document.containsField("agent")) && (!document.containsField("agentcontroller")))
                {
                    //rNode
                    region = document.field("region");
                    importRegion = region;
                    isNodeType = true;

                }
                else if((document.containsField("region")) && (document.containsField("agent")) && (!document.containsField("agentcontroller")))
                {
                    //aNode
                    region = document.field("region");
                    agent = document.field("agent");
                    isNodeType = true;

                }
                else if((document.containsField("region")) && (document.containsField("agent")) && (document.containsField("agentcontroller")))
                {
                    //pNode
                    region = document.field("region");
                    agent = document.field("agent");
                    plugin = document.field("agentcontroller");
                    isNodeType = true;
                }

                if(isNodeType) {
                    logger.debug("Importing Node : Region :" + region + " Agent :" + agent + " agentcontroller :" + plugin);

                    String nodeId = gdb.getNodeId(region, agent, plugin);


                    if (nodeId == null) {
                        //create node
                        nodeId = gdb.addNode(region, agent, plugin);
                        logger.debug("Added nodeId : " + nodeId);
                    } else {
                        logger.debug("nodeId : " + nodeId + " exist.");
                    }

                    //record node_id for cleaning
                    importedList.add(nodeId);

                    //update node params
                    
                    gdb.setNodeParams(region, agent, plugin, params);
                    logger.debug("set parmas for : " + nodeId);
                    //END

                    totalRecords++;
                }
                else {
                    logger.debug("Load failed on nonNode type Class : " + document.getClassName());
                }

            }
            record = null;
            logger.debug("Finished with Record :" + totalRecords);
        }


        logger.debug(String.format("Done. Imported %,d records in %,.2f secs\n", totalRecords,
                ((float) (System.currentTimeMillis() - begin)) / 1000));

        jsonReader.readNext(OJSONReader.COMMA_SEPARATOR);

        return totalRecords;
    }

    private ORecord importRecord() throws Exception {

        String value = jsonReader.readString(OJSONReader.END_OBJECT, true);

        // JUMP EMPTY RECORDS
        while (!value.isEmpty() && value.charAt(0) != '{') {
            value = value.substring(1);
        }
        record = null;
        try {

            try {
                record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);
            } catch (OSerializationException e) {
                if (e.getCause() instanceof OSchemaException) {
                    // EXTRACT CLASS NAME If ANY
                    final int pos = value.indexOf("\"@class\":\"");
                    if (pos > -1) {
                        final int end = value.indexOf("\"", pos + "\"@class\":\"".length() + 1);
                        final String value1 = value.substring(0, pos + "\"@class\":\"".length());
                        final String clsName = value.substring(pos + "\"@class\":\"".length(), end);
                        final String value2 = value.substring(end);

                        final String newClassName = convertedClassNames.get(clsName);

                        value = value1 + newClassName + value2;
                        // OVERWRITE CLASS NAME WITH NEW NAME
                        record = ORecordSerializerJSON.INSTANCE.fromString(value, record, null);
                    }
                } else
                    logger.error("Error on importing record");
            }

            // Incorrect record format , skip this record
            if (record == null || record.getIdentity() == null) {
                logger.error("Broken record was detected and will be skipped");
                return null;
            }

            //final ORID rid = record.getIdentity();

            //final int clusterId = rid.getClusterId();
        } catch (Exception t) {
            if (record != null)
                logger.error("Error importing record " + record.getIdentity() + ". Source line "
                        + jsonReader.getLineNumber() + ", column " + jsonReader.getColumnNumber());
            else
                logger.error("Error importing record. Source line " + jsonReader.getLineNumber() + ", column " + jsonReader.getColumnNumber());

            throw t;
        } finally {
            jsonReader.readNext(OJSONReader.NEXT_IN_ARRAY);
        }

        return record;
    }


}