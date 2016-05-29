require(recommenderlab)

mod <- 1500

raw <- read.table('training5.txt', sep=",")
raw <- raw[,2:1]
raw <- raw[raw[,2] %% mod == 0,]
data <- as(raw, "realRatingMatrix")

r <- Recommender(data, method="UBCF")

u <- 4000

recom <- predict(r, data[u], n=1000)

res <- as(recom, "list")

#print(res)

test <- read.table('test.txt', sep=",")
test <- test[,2:1]
test <- test[test[,2] %% mod == 0,]

print(sum(as.numeric(res[[1]]) %in% test[test[,1] == as.numeric(rownames(data)[u]),2]))