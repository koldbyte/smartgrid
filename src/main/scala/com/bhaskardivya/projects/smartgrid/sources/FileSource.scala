package com.bhaskardivya.projects.smartgrid.sources

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._


class FileSource {
  def getSource(env: StreamExecutionEnvironment, params: ParameterTool): DataStream[String] = {
    val filePath: String = params.get("filePath", "/run/media/osboxes/Data/data/sorted100M.1s.hsum.csv")
    val fileInterval: Long = params.getLong("fileInterval", 100)

    env.readFile(new TextInputFormat(new Path(filePath)), filePath, FileProcessingMode.PROCESS_ONCE, fileInterval)
  }

}
