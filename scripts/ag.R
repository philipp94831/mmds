files <- list.files("raw")
for (file in files) {
  print(file)
  raw <- read.table(paste0("raw/", file), sep=",")
  raw <- raw[,2:1]
  ag <- aggregate(raw, by=list(raw[,1], raw[,2]), FUN=length)
  write.table(ag[,1:3], file = paste0("final/", file), append = F, quote = F, sep=",", row.names = F, col.names = F)
}

