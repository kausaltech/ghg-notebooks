library(OpasnetUtils)
library(gsheet)

objects.latest("Op_en7783",code_name="ontology") # [[Open policy ontology]]
objects.latest("Op_en3861", code_name="makeGraph") # [[Insight network]]

make_one_entry_per_line <- function(df, col, object_list) {
  out <- data.frame()
  for(i in object_list) {
    tmp <- df[grep(i, df[[col]]),]
    tmp[[col]] <- i
    out <- rbind(out, tmp)
    df[[col]] <- gsub(i, "", df[[col]])
  }
  return(out)
}

# Try a modified version because edges do not form correctly

makeGraph <- function (ova, ...) 
{
  require(OpasnetUtils)
  require(DiagrammeR)
  if (!exists("formatted")) {
    objects.latest("Op_en3861", code_name = "formatted")
  }
  if (!exists("chooseGr")) {
    objects.latest("Op_en3861", code_name = "chooseGr")
  }
  if ("ovariable" %in% class(ova)) {
    a <- ova@output
    meta <- ova@meta$insightnetwork
  }  else {
    a <- ova
    meta <- NULL
  }
  for (i in 1:ncol(a)) {
    a[[i]] <- gsub("[\"']", " ", a[[i]])
  }
  a$label <- ifelse(is.na(a$label), substr(a$Item, 1, 30), 
                    a$label)
  a$Item <- ifelse(is.na(a$Item), a$label, a$Item)
  tst <- rep(1:nrow(a), 2)[match(a$Object, c(a$Item, a$label))]
  hasobj <- !(is.na(a$Object) | a$Object == "")
#  a$Object[hasobj] <- a$Item[tst][hasobj]
  newbies <- ifelse(is.na(tst), a$Object, NA)
  newbies <- newbies[!is.na(newbies)]
  if (length(newbies) > 0) {
    a <- orbind(a, data.frame(Item = newbies, label = substr(newbies, 
                                                             1, 30), stringsAsFactors = FALSE))
  }
  nodes <- a[!(duplicated(a$Item) | is.na(a$Item) | a$Item == 
                 ""), ]
  nodes$tooltip <- paste0(nodes$label, ". ", ifelse(nodes$label == 
                          nodes$Item, "", paste0(nodes$Item, ". ")), ifelse(is.na(nodes$Description), 
                          "", paste0("\n", nodes$Description)), " (", nodes$Context, 
                          "/", nodes$id, ")")
  nodes <- merge(nodes, formatted, by.x = "type", by.y = "Resource", all.x = TRUE)
  colnames(nodes) <- gsub("node.", "", colnames(nodes))
  nodes <- nodes[!grepl("edge.", colnames(nodes))]
  nodes$id <- 1:nrow(nodes)
  inver <- opbase.data("Op_en7783", subset = "Relation types")
  for (i in colnames(inver)) inver[[i]] <- as.character(inver[[i]])
  inve <- data.frame(rel = c(inver$`English name`, inver$`Finnish name`), 
                     inve = c(inver$`English inverse`, inver$`Finnish inverse`), 
                     stringsAsFactors = FALSE)
  edges <- a[!(is.na(a$Object) | a$Object == ""), ]
  flip <- edges$rel %in% inve$inve
  tmp <- edges$Item
  edges$Item[flip] <- edges$Object[flip]
  edges$Object[flip] <- tmp[flip]
  edges$rel[flip] <- inve$rel[match(edges$rel, inve$inve)][flip]
  edges$from <- match(edges$Item, nodes$Item)
  edges$to <- match(edges$Object, nodes$Item)
  edges$label <- edges$rel
  edges$labeltooltip <- paste0(edges$label, " (", edges$Context, 
                               "/", edges$id, ")")
  edges <- merge(edges, formatted, by.x = "rel", by.y = "Resource", all.x = TRUE)
  colnames(edges) <- gsub("edge.", "", colnames(edges))
  edges <- edges[!grepl("node.", colnames(edges))]
  edges$id <- 1:nrow(edges)
  gr <- create_graph(nodes_df = nodes, edges_df = edges)
  if (!is.null(meta)) {
    gr <- chooseGr(gr, input = meta)
  }
  return(gr)
}

# TAMPERE
df <- gsheet2tbl("https://docs.google.com/spreadsheets/d/14IqbQO466LiZW84VnLgvLRx29DnI2bjnGTu2T6M8aZ0/edit#gid=1688281028")
#colnames(df)
#[1] "Tyyppi" "Nimi"   "Teemat" "SDG"    "EGCA"  
colnames(df) <- c("Tyyppi", "Item","Object","SDG","EGCA")
df$label <- substr(df$Item,1,30)
df$Description <- df$Item
df$Context <- "Tampere"
df$id <- 1:nrow(df)
df$rel <- "instance of"

df$type <- ifelse(df$Tyyppi == "Taktinen mittari", "toiminnallinen mittari","vaikuttavuusmittari")

gr <- makeGraph(df)
render_graph(gr, title="Tampere indicators with own classification")

df$Object2 <- df$Object
df$Object <- df$SDG
TRE_TYPES <- c("15.1.1", "15.2.1", "7.1.2", "7.2.1", "7.3.1", "12.2.1", "12.2.2")
df <- make_one_entry_per_line(df, "Object", TRE_TYPES)
#  df$Object <- paste("SDG", df$Object)

gr <- makeGraph(df)
render_graph(gr, title = "Tampere indicators with SDG connections")
#  export_graph(gr, "Tampere indicators with SDG connections.png")

df_tampere_sdg <- df[df$Object!="-",]
df_tampere_sdg$type <- "decision"

############################################3
# HELSINKI
df <- gsheet2tbl("https://docs.google.com/spreadsheets/d/14IqbQO466LiZW84VnLgvLRx29DnI2bjnGTu2T6M8aZ0/edit#gid=1446230545")
#colnames(df)
#[1] "Tyyppi" "Nimi"   "Teemat" "SDG"    "EGCA"  
colnames(df)[2:3] <- c("Item","Object")
df$label <- substr(df$Item,1,30)
df$Description <- df$Item
df$Context <- "Helsinki"
df$rel <- "instance of"
df$type <- ifelse(df$Tyyppi=="Toiminnallinen mittari", "operational indicator",
                  ifelse(df$Tyyppi=="Taktinen mittari", "tactical indicator", "strategic indicator"))

HKI_TYPES <- c(
  "Välittömät päästöt",
  "Välilliset päästöt",
  "Täydentyvä kaupunkirakenne",
  "Liikenne",
  "Uudet energiaratkaisut",
  "Smart & Clean kasvu – uusia työpaikkoja ja liiketoimintaa Helsinkiin",
  "Seuranta ja raportointi",
  "Tontinluovutus",
  "Uudet liikkumispalvelut ja liikkumisen ohjaus",
  "Viestintä ja osallistuminen",
  "Ilmastotyön koordinointi, seuranta ja arviointi",
  "Ilmastotyön koordinointi",
  "Seuranta ja raportointi",
  "Sataman päästöjen vähentäminen",
  "Rahoitus ja kannustimet",
  "Rakennusvalvonta",
  "Palvelurakennukset",
  "Asuinrakennukset",
  "Rakentaminen ja rakennusten käyttö",
  "Kestävien kulkumuotojen käyttö",
  "Liikenteen hinnoittelu",
  "Ajoneuvoteknologian muutokset",
  "Asemakaavoitus",
  "Kuluttaminen, hankinnat, jakamis- ja kiertotalous",
  "Rakentamisen hiilijalanjäljen pienentäminen ja puurakentaminen",
  "Hankinnat",
  "Kasvatus ja koulutus",
  "Kaupungin omistamat asuin- ja palvelurakennukset sekä ulkovalaistus",
  "Kuluttaminen ja jätteet",
  "Kaupunkiympäristö ja -tarjonta",
  "Hiilinielut ja päästöjen kompensointi",
  "Hiilinielut",
  "Jakamis- ja kiertotalous",
  "Energiatehokas maankäyttö ja kaupunkirakenne",
  "Kaupungin omistamat rakennukset",
  "Energiarenessanssi"
)

gr <- makeGraph(make_one_entry_per_line(df, "Object", HKI_TYPES))
render_graph(gr, "Helsinki indicators with own classification")

#########################################3
# SDG
df <- gsheet2tbl("https://docs.google.com/spreadsheets/d/14IqbQO466LiZW84VnLgvLRx29DnI2bjnGTu2T6M8aZ0/edit#gid=1539476707")
df$ID <- gsub("Target ", "", df$ID)

out <- character()
for(i in 1:nrow(df)) {
  out <- c(out, paste(strsplit(df$ID[i], "\\.")[[1]][1:(df$Level[i]-1)], collapse="."))
}
df$Object <- out
df$Item <- df$ID
df$label <- df$ID #substr(df$Description,1,30)
df$type <- factor(df$Level, labels=c("objective", "strategic indicator", "tactical indicator"))
df$rel <- "instance of"
df$Context <- "SDG"

gr <- makeGraph(df)
render_graph(gr, title = "UN Sustainable Development Goals (SDG)")

gr <- makeGraph(orbind(df_tampere_sdg, df))
render_graph(gr, title="Tampere indicators among SDG")

###########################################3
# LAHTI

LAHTI_TYPES <- c(
  "Kestävä talous",
  "Sopeutuminen",
  "Kestävä kaupunkisuunnittelu",
  "Vedet ja vesistöt",
  "Kiertotalous",
  "Osallistuminen",
  "Liikenne",
  "Hillintä",
  "Ilmanlaatu",
  "Luonnon monimuotoisuus",
  "Melu"
)

df <- gsheet2tbl("https://docs.google.com/spreadsheets/d/14IqbQO466LiZW84VnLgvLRx29DnI2bjnGTu2T6M8aZ0/edit#gid=308152450")
df$Object <- df$Teemat
df <- make_one_entry_per_line(df, "Object", LAHTI_TYPES)
df$Item <- df$Nimi
df$label <- df$Nimi
df$Context <- "Lahti"
df$rel <- "instance of"
df$type <- ifelse(df$Tyyppi=="Strateginen mittari","strategic indicator",
                  ifelse(df$Tyyppi=="Taktinen mittari","tactical indicator","operational indicator"))

gr <- makeGraph(df)
render_graph(gr, title = "Lahti indicators")

###########################################3
# Newcastle

NEWCASTLE_ORGS <- c(
  "all city residents, young people",
  "Bus operators",
  "business forums", # redundant 3-->11
  "Citizen Assembly", # redundant 4-->5
  "Citizen's Assembly",
  "City and regional transport organisations",
  "city centre businesses and other organisations", # redundant 7-->11
  "city partners",
  "city-wide businesses", # redundant 9-->11
  "city-wide businesses and employers", # redundant 10-->11
  "businesses",
  "Climate Change Adaptation Working Group",
  "E.ON",
  "employers and business forums", # redundant 14-->11
  "Environment Agency",
  "freight companies",
  "Gateshead Council",
  "Government / Office of National Statistics",
  "Groundwork",
  "Invest Newcastle",
  "local businesses", # redundant 21-->11
  "local schools",
  "local training and skills providers",
  "NE1",
  "Newcastle City Concil", # redundant 25-->26
  "Newcastle City Council",
  "Newcastle College Group",
  "Newcastle Gateshead Initiative",
  "Newcastle International Airport",
  "Newcastle University",
  "Newcastle upon Tyne Hospitals NHS Foundation Trust",
  "Nexus",
  "North East Combined Authority",
  "North East Local Enterprise Partnership",
  "North of Tyne Combined Authority",
  "Northern Gas Network",
  "Northern Powergrid",
  "Northumbria University",
  "Northumbrian Water",
  "Ofgem",
  "local authorities",
  "other public sector bodies", # redundant 42-->41
  "other relevant interested stakeholders",
  "other research bodies",
  "partner authorities in Gateshead, North Tyneside, Northumberland and Sunderland",
  "partner organisations for pilot project",
  "Regenerate Newcastle Partnership",
  "together with neighbouring local authorities", #redundant 48-->41
  "trade unions",
  "Transport for the North, National Rail, local maritime sector",
  "transport operators",
  "Tyne and Wear Pensions Fund",
  "Urban Green",
  "Your Homes Newcastle and other housing associations"
)

df <- gsheet2tbl("https://docs.google.com/spreadsheets/d/14IqbQO466LiZW84VnLgvLRx29DnI2bjnGTu2T6M8aZ0/edit#gid=31511668")
df <- make_one_entry_per_line(df, "Responsible parties", NEWCASTLE_ORGS)

df$`Responsible parties` <- ifelse(
  df$`Responsible parties` %in% NEWCASTLE_ORGS[c(3,7,9,10,21)],
  df$`Responsible parties`[11], df$`Responsible parties`)
df$`Responsible parties` <- ifelse(
  df$`Responsible parties` %in% NEWCASTLE_ORGS[c(4)],
  df$`Responsible parties`[5], df$`Responsible parties`)
df$`Responsible parties` <- ifelse(
  df$`Responsible parties` %in% NEWCASTLE_ORGS[c(25)],
  df$`Responsible parties`[26], df$`Responsible parties`)
df$`Responsible parties` <- ifelse(
  df$`Responsible parties` %in% NEWCASTLE_ORGS[c(42,48)],
  df$`Responsible parties`[41], df$`Responsible parties`)

df$Item <- df$ID
df$Object <- df$`Responsible parties`
df$label <- df$ID
df$Description <- substr(df$Action,1,30)
df$Context <- "Newcastle"
df$rel <- "responsibility of"
df$type <- ifelse(grepl("&S",df$ID),"strategic indicator",
                  ifelse(grepl("T",df$ID),"tactical indicator",
                               ifelse(grepl("E",df$ID),"operational indicator","risk factor")))

gr <- makeGraph(df)
render_graph(gr, title = "Newcastle action responsibilities are really focussed around Newcastle City Council")

gr <- makeGraph(df[df$`Responsible parties`!="Newcastle City Council",])
render_graph(gr, title="Newcastle action responsibilities without the City Council")

#######################################
# Umeå

#df <- gsheet2tbl("https://docs.google.com/spreadsheets/d/14IqbQO466LiZW84VnLgvLRx29DnI2bjnGTu2T6M8aZ0/edit#gid=815059680")

txt <- readLines("~/Umeå.txt")
txt <- trimws(txt)
txt <- txt[txt!=""]
BLOCK_BREAKS <- grep("###",txt)
sort(txt[BLOCK_BREAKS+1])

n <- function(x) {
  if(length(x)==0) x <- NA
  return(x)
}
out <- data.frame()
for(i in 1:(length(BLOCK_BREAKS)-1)) {
  tmp <- txt[BLOCK_BREAKS[i]:BLOCK_BREAKS[i+1]]
#  print(tmp)
  NAME <- tmp[2]
  DESCRIPTION <- paste(tmp[3:(match("info_outline", tmp)-1)],collapse="\n")
  TYPE <- tmp[grep("filter_none",tmp)+1]
  STATUS <- tmp[grep("STATUS",tmp)+1]
  ANSVARIG_ORGANISATION <- tmp[grep("ANSVARIG ORGANISATION",tmp)+1]
  KOPPLAD_TILL <- tmp[(grep("KOPPLAD TILL:",tmp)+1):(length(tmp)-1)]
  IMPLEMENTERAS <- tmp[grep("IMPLEMENTERAS",tmp)+1]
  REFERENSER <- tmp[grep("REFERENSER",tmp)+1]
  out <- rbind(out,data.frame(
    Name = n(NAME),
    Description = n(DESCRIPTION),
    Type = n(TYPE),
    Status = n(STATUS),
    Responsible = n(ANSVARIG_ORGANISATION),
    Targets = n(KOPPLAD_TILL),
    Implementation = n(IMPLEMENTERAS),
    References = n(REFERENSER)
  ))
}
umeå_actions <- out

##############################
# Umeå categories

txt <- readLines("~/Umeå2.txt")
txt <- trimws(txt)
txt <- txt[txt!=""]
BLOCK_BREAKS <- grep("###",txt)
i <- 1
out <- data.frame()
for(i in 1:(length(BLOCK_BREAKS)-1)) {
  tmp <- txt[BLOCK_BREAKS[i]:BLOCK_BREAKS[i+1]]
  NAME <- tmp[2]
  CATEGORY1 <- tmp[16]
  CATEGORY2 <- tmp[17]
  CATEGORY3 <- tmp[18]
  if(CATEGORY1 %in% c("Avfall","Industri","Jordbruk","Övrigt (Arbetsmaskiner, produkter mm)")) CATEGORY3 <- NA
  MAL <- grep("Mål 2030",tmp)
  if(NAME=="---") {
    NAME <- tmp[MAL-1]
    if(grepl("%",NAME)) NAME <- tmp[MAL-2]
  }
  MALDESC <- tmp[MAL+1]
  DESC <- tmp[(MAL+2):(grep("Genomförda åtgärder",tmp)-1)]
  DESC <- paste(DESC[!DESC %in% c(NAME, "Mer information")], collapse="\n")

  out <- rbind(out, data.frame(
    Name = NAME,
    Category1 = CATEGORY1,
    Category2 = CATEGORY2,
    Category3 = CATEGORY3,
    Goal = MALDESC,
    Description = DESC
  ))
}

if(FALSE) {
ontology <- EvalOutput(ontology)
ontology@meta$insight <- list(
  steps=1,
  language = lang
)

gr<- makeGraph(ontology)
# export_graph(gr, "Open policy ontology network.svg")

gr2 <- select_nodes_by_id(gr, 148) # Items
for(i in 1:9) {gr2 <- trav_out(gr2, add_to_selection=TRUE)}
gr2 <- transform_to_subgraph_ws(gr2)
render_graph(gr2)
#export_graph(gr2, "Open policy ontology network items.png")

gr2 <- select_nodes_by_id(gr, 228) # Relations
for(i in 1:9) {gr2 <- trav_out(gr2, add_to_selection=TRUE)}
gr2 <- transform_to_subgraph_ws(gr2)
render_graph(gr2)
#export_graph(gr2, "Open policy ontology network relations.png")
}
