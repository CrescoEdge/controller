package io.cresco.agent.controller.communication;

import io.cresco.library.data.FileObject;
import io.cresco.library.messaging.MsgEvent;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileObjectGroupReceiver {

    private Map<String, FileObject> fileObjectMap;
    private MsgEvent me;
    private String fileGroup;
    private List<String> fileCompleteList;

    public FileObjectGroupReceiver(MsgEvent me, Map<String, FileObject> fileObjectMap, String fileGroup) {
        this.fileObjectMap = fileObjectMap;
        this.me = me;
        this.fileGroup = fileGroup;
        fileCompleteList = new ArrayList<>();
    }

    public List<String> getFileList(Path journalPath) {
        List<String> fileList = null;
        try {

            fileList = new ArrayList<>();

            for (Map.Entry<String, FileObject> entry : fileObjectMap.entrySet()) {
                String key = entry.getKey();
                FileObject value = entry.getValue();
                Path filePath = Paths.get(journalPath.toAbsolutePath() + System.getProperty("file.separator") + key + System.getProperty("file.separator") + value.getFileName());
                fileList.add(filePath.toAbsolutePath().toString());
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fileList;
    }

    public MsgEvent getMsgEvent() {
        return me;
    }

    public boolean setDestFilePart(String dataName, String filePartName, String filePartMD5Hash) {
        boolean isSetPart = false;
        if(fileObjectMap.containsKey(dataName)) {
            fileObjectMap.get(dataName).setDestFilePart(filePartName,filePartMD5Hash);
        }
        return isSetPart;
    }

    public boolean isFilePartComplete(String dataName) {
        boolean isComplete = false;
        try {
            if(fileObjectMap.containsKey(dataName)) {
                isComplete = fileObjectMap.get(dataName).isFilePartComplete();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isComplete;
    }

    public void setFileComplete(String dataName) {
        fileCompleteList.add(dataName);
    }

    public List<String> getOrderedPartList(String dataName) {
        List<String> orderedPartList = null;
        try {

            if(fileObjectMap.containsKey(dataName)) {
                orderedPartList = fileObjectMap.get(dataName).getOrderedPartList();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return orderedPartList;
    }

    public String getFileName(String dataName) {
        String fileName = null;
        try {
            if(fileObjectMap.containsKey(dataName)) {
                fileName = fileObjectMap.get(dataName).getFileName();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fileName;
    }

    public String getFileMD5Hash(String dataName) {
        String fileHash = null;
        try {
            if(fileObjectMap.containsKey(dataName)) {
                fileHash = fileObjectMap.get(dataName).getFileMD5Hash();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fileHash;
    }


    public boolean isFileGroupComplete() {
        boolean isComplete = false;
        try {
            if(fileObjectMap.size() == fileCompleteList.size()) {

                boolean isFault = false;
                for (String key : fileObjectMap.keySet()) {
                    if (!fileCompleteList.contains(key)) {
                        isFault = true;
                        break;
                    }
                }

                if(!isFault) {
                    isComplete = true;
                }


            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return isComplete;
    }


}
