# This is code Expert_elicitation_analysis.R on ~/devel/nme_nowcasting/

library(gsheet)
library(abind)
library(reshape2)
library(ggplot2)

if(FALSE) {
  dt <- gsheet::gsheet2tbl(
    "https://docs.google.com/spreadsheets/d/1PcRgKgX4XzsEnqyKMxrd4mrPjn4wn3XH3zjx6Z7vuAo/edit?usp=sharing")
  dt <- dt[!is.na(dt$Timestamp),]
  dt <- dt[-c(1,2,3,5,6,7,17),]
  
  questions <- c("Q1","Q2","Q3","Q4")
  answers <- c(177.5, 12000, 5530, (1-11700/12000)*100)  # https://ilmastoruoka.test.kausal.tech/node/net_emissions
  quantiles <- c("0 %","5 %","50 %","95 %","100 %")
  bins <- c("0-5","5-50","50-95","95-100")
  probs <- c(0.05,0.45,0.45,0.05)
  
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
}

# Example from Cooke: Experts in uncertainty, ESTEC-1 on page 246
dt <- gsheet::gsheet2tbl("https://docs.google.com/spreadsheets/d/1PcRgKgX4XzsEnqyKMxrd4mrPjn4wn3XH3zjx6Z7vuAo/edit#gid=1472341596")

dt$Expert <- paste0("E", dt$Expert)
colnames(dt)[colnames(dt)=="Expert"] <- "Name"
dt$Question <- paste0("Q", dt$Question)
dt$Question <- ifelse(nchar(dt$Question)==2, gsub("Q", "Q0", dt$Question), dt$Question)

names <- unique(dt$Name)
questions <- unique(dt$Question)
answers <- dt$Realization[1:length(questions)]
quantiles <- c("0 %","5 %","50 %","95 %","100 %")
bins <- c("0-5","5-50","50-95","95-100")
probs <- c(0.05,0.45,0.45,0.05)
logs <- dt$Logarithmic[1:length(questions)]

answers[logs] <- log10(answers[logs])

ar <- melt(dt[1:5], variable.name = "Quantile", id.vars = c("Name","Question"))
ar <- ar[order(ar$Question, ar$Quantile, ar$Name) , ]

ar <- array(data = ar$value,
            dim = c(length(names), length(quantiles)-2, length(questions)),
            dimnames = list(Names = names, Quantile = quantiles[2:4], Questions = questions))

ar[,,logs] <- log10(ar[,,logs])

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


counts <- array(0, dim = c(length(names), length(bins)), dimnames = list(Name = names, Bin = bins))

counts <- array(c(
  ifelse(answers < ar[,"5 %",], 1, 0),
  ifelse(answers >= ar[,"5 %",] & answers < ar[,"50 %",], 1, 0),
  ifelse(answers >= ar[,"50 %",] & answers < ar[,"95 %",], 1, 0),
  ifelse(answers >= ar[,"95 %",], 1, 0)
),
dim = c(dim(answers), 4),
dimnames = c(dimnames(answers), Bin = list(bins)))

counts <- apply(counts, MARGIN = c(1,3), FUN = sum)

realization <- dt[!is.na(dt$Realization) , c("Question","Realization")]
get_s_i <- function(
  df,  # data as dataframe
  realization,  # dataframe with realizations
  realization_name = "Realization",
  expert_name = "Expert",  # names in the df for experts, questions, and probabilities
  question_name = "Question",
  value_name = "value",
  probability_name = "Probability",
  probability_names = c("P00", "P05", "P50", "P95", "P100"),
  probabilities = c(0, 0.05, 0.5, 0.95, 1)
) {
  n_questions <- length(unique(df[[question_name]]))
  bin_names <- character()
  boundaries <- data.frame(
    p = probability_names[c(1, length(probability_names))],
    v = c(-Inf, Inf))
  colnames(boundaries) <- c(probability_name, value_name)
  
  for(i in 1:(length(probabilities)-1)) {
    bin_names <- c(bin_names, paste(probability_names[i], probability_names[i + 1], sep = "-"))
  }
  s_i <- data.frame()
  for(i in unique(df[[expert_name]])) {
    s_ie <- rep(0, length(bin_names))
    for(j in unique(df[[question_name]])) {
      realiz <- realization[realization[[question_name]] == j , realization_name]
      tmp <- rbind(boundaries, 
                   df[df[[expert_name]] == i & df[[question_name]] == j , c(probability_name, value_name)]
      )
      for(k in 1:length(bin_names)) {
        if(
          realiz >= tmp[tmp[[probability_name]] == probability_names[k] , value_name] &
          realiz < tmp[tmp[[probability_name]] == probability_names[k + 1] , value_name]) {
          s_ie[k] <-s_ie[k] + 1
        }
      }
    }
    s_i <- rbind(s_i, data.frame(Expert = i, S_i = t(s_ie / n_questions)))
  }
  colnames(s_i)[2:ncol(s_i)] <- bin_names
  return(s_i)
}

s_i <- get_s_i(df, realization)

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

p_r <- array(rep(probs,
          each = length(names) * length(questions)),
      dim = c(length(names), length(questions), length(bins)),
      dimnames = list(Name = names, Question = questions, Bin = bins))

informativeness <- apply(p_r * log(p_r / widths), MARGIN = 1:2, FUN = sum)
informativeness <- apply(log(range) + informativeness, MARGIN = 1, FUN = sum) / length(questions)

calibration <- s_i * log(s_i / apply(p_r, MARGIN = c(1,3), FUN = mean))
calibration[is.nan(calibration)] <- 0
calibration <- apply(2 * counts * calibration, MARGIN = 1, FUN = sum)
calibration <- 1 - pchisq(calibration, df = length(bins))

get_calibration <- function(
  s_i,  # sample distribution of variables in bin i
  q,  # number of questions
  p_i = c(5, 45, 45, 5)/100,  # expected distribution of variables in bin i
  df = 3  # degrees of freedom
) {
  if(length(s_i) != length(p_i)) {errorCondition("s_i and p_i are of different length")}
  C_e <- s_i * log(s_i / p_i)
  print(s_i)
  print(p_i)
  print(C_e)
  C_e[is.nan(C_e)] <- 0
  C_e <- 1 - pchisq(2 * q * sum(C_e), df)
  return(C_e)
}

get_calibration(unlist(s_i[4,2:5]), 13)
get_calibration(c(.1, .2, .5, .2), 10)
get_calibration(c(2,0,1,2)/5, 5)
get_information <- function(
  quantiles,  # quantiles given by an expert
  range,  # range covering all experts' quantiles + overshoot
  p_i = c(5,45,45,5)/100  # expected distribution of variables in bin i
) {
  if(length(quantiles) + length(range) != length(p_i) + 1) {errorCondition("quantiles or p_i has wrong length")}
  quant <- c(range[1], quantiles, range[2])
  print(quant)
  I_e = 0
  for(i in 1:length(p_i)) {
    I_e <- I_e + p_i[i] * log(p_i[i] / (quant[i+1] - quant[i]))
  }
  I_e <- I_e + log(quant[length(p_i) + 1] - quant[1])
  return(I_e)
}



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
