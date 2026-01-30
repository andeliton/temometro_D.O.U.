# setup.R
# Instala dependências do Termômetro DOU

pacotes <- c(
  "shiny", "bslib", "tidyverse", "arrow", 
  "plotly", "waiter", "rvest", "xml2", "httr", "fs"
)

novos <- pacotes[!(pacotes %in% installed.packages()[,"Package"])]
if(length(novos)) install.packages(novos)