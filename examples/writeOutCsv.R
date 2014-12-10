## config hadoop
source("/home/jpoullet/Rscript/testing/config.R")

library(rmr2)

myhdfsfile = "/user/jpoullet/testing/mydata.csv"
outputDir ="/user/jpoullet/testing/res/"

split.data = mapreduce(
  input = myhdfsfile,
  input.format = make.input.format("csv", sep = ","),
  map = function(.,data){
    keyval(data[,1,drop=FALSE],data[,4,drop=FALSE])
  },
  output = outputDir,
  output.format = make.output.format("csv",sep=",")
)
