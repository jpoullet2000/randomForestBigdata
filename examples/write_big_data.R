source("/home/jpoullet/Rscript/testing/config.R")

library(rhdfs)
hdfs.init()

write.big.matrix <-function(filelist,outfile,tohdfs=TRUE,nmax=100,header=TRUE,sep=","){
  #' @param filelist list of csv files stored in HDFS 
  #' @param outfile hdfs file to write the concatenate matrix to
  #' @param tohdfs flag which tells if the written file (outfile) is in HDFS or not 
  #' @param nmax number of rows that are written at each iteration
  #' @param header header in the snpStats matrix 
  #' @param sep separation character
  
  if (tohdfs) {
    testf <- hdfs.file(outfile,"w")
  }else{
    if(file.exists(outfile)) unlink(outfile) 
    testf <- file(outfile, "at")   # or "wt" - "write text" 
    on.exit(close(testf)) 
  }
  
  reader = list()
  
  # looping
  for (f in filelist){
    reader[[f]] = hdfs.line.reader(f,n=nmax)
#     S[[f]] = read.table(textConnection(x),header=header,sep=sep)
  }


  cont = 1
  #cc = 0
  while (1){
    # if (cc == 2) break

    tc = c()
    for (f in filelist){
      x <- reader[[f]]$read()
      if (length(x) == 0) {
        cont = 0 
        break
      }
      tc <- paste(tc,x,sep=',')
    }
    if (cont == 0) break
    if (tohdfs){
      hdfs.write(tc,testf) 
    }else{
      writeLines(tc, testf) 
    }
    # cc = cc + 1
  }
  hdfs.close(testf)
 
}

filelist <- paste("/user/gbonte/allp.",c(1,1),".csv",sep="")
#outfile <- "/home/jpoullet/data/alls.csv"
outfile <- "/user/jpoullet/data/alls.csv"
write.big.matrix(filelist,outfile,tohdfs=TRUE,nmax=100,header=TRUE,sep=",")
