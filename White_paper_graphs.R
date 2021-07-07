library(OpasnetUtils)
library(gsheet)

objects.latest("Op_en7783",code_name="ontology") # [[Open policy ontology]]
objects.latest("Op_en3861", code_name="makeGraph") # [[Insight network]]

make_one_object_per_line <- function(df, object_list) {
  out <- data.frame()
  for(i in object_list) {
    tmp <- df[grep(i, df$Object),]
    tmp$Object <- i
    out <- rbind(out, tmp)
    df$Object <- gsub(i, "", df$Object)
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

if(FALSE) {
  df$Object2 <- df$Object
  df$Object <- df$SDG
  TRE_TYPES <- c("15.1.1", "15.2.1", "7.1.2", "7.2.1", "7.3.1", "12.2.1", "12.2.2")
  df <- make_one_object_per_line(df, TRE_TYPES)
  df$Object <- paste("SDG", df$Object)
  gr <- makeGraph(df)
  render_graph(gr, title = "Tampere indicators with SDG connections")
  export_graph(gr, "Tampere indicators with SDG connections.png")
}

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

gr <- makeGraph(make_one_object_per_line(df, HKI_TYPES))
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
