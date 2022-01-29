# This is code Expert_elicitation_analysis.R on ~/devel/nme_nowcasting/

library(gsheet)
library(abind)
library(reshape2)

dt <- gsheet::gsheet2tbl(
  "https://docs.google.com/spreadsheets/d/1PcRgKgX4XzsEnqyKMxrd4mrPjn4wn3XH3zjx6Z7vuAo/edit?usp=sharing")
dt <- dt[!is.na(dt$Timestamp),]
dt <- dt[-c(1,2,3,5,6,7,17),]

questions <- c("Q1","Q2","Q3","Q4")
answers <- c(177.5, 12000, 5530, (1-11700/12000)*100)  # https://ilmastoruoka.test.kausal.tech/node/net_emissions
quantiles <- c("0 %","5 %","50 %","95 %","100 %")
bins <- c("0-5","5-50","50-95","95-100")

names <- dt$Name
names <- ifelse(is.na(names), paste0("R", 1:length(names)), names)

ar <- (array(as.numeric(unlist(dt[paste0(rep(questions,each=3), c("L","M","U"))])),
         dim = c(nrow(dt),3,length(questions)),
         dimnames = list(Name = names, Quantile = quantiles[2:4],
            Question = questions)))

questions <- c(
  "Height (cm)",
  "Total food emission (kt CO2e)",
  "Meat emission (kt CO2e)",
  "Change due to meat reduction (%)"
)
dimnames(ar)$Question <- questions
cuts <- array(0, dim = c(length(names), 2, length(questions)),
              dimnames = list(Name = names, Quantile = c("0 %","100 %"), Question = questions))

for(i in questions) {
  cut <- as.vector(ar[,,i])
  drift <- (max(cut) - min(cut)) * 0.1
  cuts[,1,i] <- min(cut) - drift
  cuts[,2,i] <- max(cut) + drift
}

ar <- abind(ar, cuts, along = 2)

answers <- array(rep(answers, each=length(names)), 
                 dim = c(length(names), length(questions)),
                 dimnames = list(Name = names, Question = questions))


counts <- array(0, dim = c(nrow(dt), 4), dimnames = list(Name = names, Bin = bins))

counts <- array(c(
  ifelse(answers < ar[,"5 %",], 1, 0),
  ifelse(answers >= ar[,"5 %",] & answers < ar[,"50 %",], 1, 0),
  ifelse(answers >= ar[,"50 %",] & answers < ar[,"95 %",], 1, 0),
  ifelse(answers >= ar[,"95 %",], 1, 0)
),
dim = c(dim(answers), 4),
dimnames = c(dimnames(answers), Bin = list(bins)))

counts <- apply(counts, MARGIN = c(1,3), FUN = sum)

binprobs <- array(rep(c(0.05, 0.45, 0.45, 0.05), each = length(names)),
                   dim = c(length(names), length(bins)),
                   dimnames = list(Name = names, Bin = bins))

range <- array(ar[,"100 %",] - ar[,"0 %",], dim = c(length(names), length(questions)),
               dimnames = list(Name = names, Question = questions))

widths <- abind(
  `0-5` = ar[,"5 %",] - ar[,"0 %",],
  `5-50` = ar[,"50 %",] - ar[,"5 %",],
  `50-95` = ar[,"95 %",] - ar[,"50 %",],
  `95-100` = ar[,"100 %",] - ar[,"95 %",],
  along = 3
)

p_r <- array(rep(c(0.05,0.45,0.45,0.05),
          each = length(names) * length(questions)),
      dim = c(length(names), length(questions), length(bins)),
      dimnames = list(Name = names, Question = questions, Bin = bins))

informativeness <- apply(p_r * log(p_r / widths), MARGIN = 1:2, FUN = sum)
informativeness <- apply(log(range) + informativeness, MARGIN = 1, FUN = sum) / length(questions)

s_i <- counts / length(questions)
calibration <- s_i * log(s_i / apply(p_r, MARGIN = c(1,3), FUN = mean))
calibration[is.nan(calibration)] <- 0
calibration <- apply(2 * counts * calibration, MARGIN = 1, FUN = sum)
calibration <- 1 - pchisq(calibration, df = length(bins))

scores <- data.frame(
  calibration = calibration,
  informativeness = informativeness,
  score = calibration * informativeness
)

sample <- data.frame()

for(i in names) {
  for(j in questions) {
    tmp <- ar[i,,j]
    for(k in 1:4) {
      sample <- rbind(sample, data.frame(
        Name = i,
        Question = j,
        Value = runif(100 * p_r[1,1,k], tmp[quantiles[k]],
                      tmp[quantiles[k + 1]])
      ))
    }
  }
}

CI90 <- melt(ar[,c("5 %","95 %"),])
colnames(CI90) <- c("Name","Probability","Question","Value")

ggplot(sample, aes(x = Name, y = Value))+geom_boxplot(outlier.shape = NA)+
  geom_hline(data = data.frame(Question = questions, Value = answers[1,]), color="red", aes(yintercept = Value))+
  geom_point(data = CI90, aes(x = Name, y = Value, color = Probability))+
  facet_wrap(~ Question, scales = "free")+
  coord_flip()+
  scale_color_manual(values = c("turquoise", "green"))+
  labs(title = "Training for estimating subjective probabilities (box contains 25 %, 50 %, and 75 % probabilities)")

ggsave("AI4Cities_Subjective_probabilities.pdf", width = 11, height = 7.5)
