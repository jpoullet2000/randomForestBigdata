library(randomForest)
data(iris)
irishdfs= to.dfs(iris)

frac.per.model <- 0.1
num.models <- 50
model.formula <- Species ~ Sepal.Length + Sepal.Width + Petal.Length + Petal.Width
## subsampling (for mapping)
fixed.row.subsample <- function(k,input){
  generate.sample <- function(i){
    indices <- sample(nrow(input),frac.per.model*nrow(input))
    keyval(i, input[indices,])
  }
  c.keyval(lapply(1:num.models, generate.sample))
}

poisson.subsample <- function(k, input) {
  generate.sample <- function(i) {
    draws <- rpois(n=nrow(input), lambda=frac.per.model)
    indices <- rep((1:nrow(input)), draws)
    keyval(i, input[indices, ])}
  c.keyval(lapply(1:num.models, generate.sample))}

## modeling
fit.trees <- function(k, v) {
  rf <- randomForest(formula=model.formula, data=v, na.action=na.roughfix, ntree=10, do.trace=FALSE)
  keyval(k, list(forest=rf))}

## mapreduce
rfr = from.dfs(
  mapreduce(input=irishdfs, 
            map=fixed.row.subsample, 
            reduce=fit.trees, 
  )
)

rfr = from.dfs(
  mapreduce(input=irishdfs, 
          map=poisson.subsample, 
          reduce=fit.trees, 
          )
)
