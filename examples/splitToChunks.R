## config hadoop
source("/home/jpoullet/Rscript/testing/config.R")

library(rmr2)

myhdfsfile = "/user/gbonte/IBDC/all.IBDC.68427.27500.1.csv"

bp = rmr.options("backend.parameters");
bp$hadoop[1] = "mapreduce.map.java.opts=-Xmx1024M";
bp$hadoop[2] = "mapreduce.reduce.java.opts=-Xmx2048M";
bp$hadoop[3] = "mapreduce.map.memory.mb=6024";

nchunk = 10 # number of chunks per split

split.data = mapreduce(
  input = myhdfsfile,
  input.format = make.input.format("csv", sep = ","),
  map = function(.,data){
    nr = nrow(data)
    nrPerChunk = ceiling(nr/nchunk)
    keyval(1:nchunk,lapply(1:nchunk,function(x) data[((x-1)*nrPerChunk+1):min(x*nrPerChunk,nr),]))
  },
  backend.parameters = bp
)


# count the number of chunks
nbr.chunks = mapreduce(
  input = split.data,
  map = function(chunk,data){
    keyval(1,1)
  },
  reduce = function(k,v) keyval(sum(v)),
  backend.parameters = bp
)

number.of.chunks = from.dfs(nbr.chunks)
