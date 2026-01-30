# ==============================================================================
# ETL 02: PROCESSADOR DE XML (ATOS DE PESSOAL)
# ==============================================================================

library(tidyverse)
library(xml2)
library(arrow)
library(fs)
library(lubridate)

print("⚙️ INICIANDO PROCESSAMENTO DE DADOS...")

# 1. CONFIGURAÇÃO
DIR_ZIPS     <- "dados_zips_download"
DIR_TEMP     <- "temp_xml_extract"
ARQUIVO_FINAL <- "base_dou_dashboard.parquet"

if(!dir.exists(DIR_TEMP)) dir.create(DIR_TEMP)

# 2. REGEX
regex_nomeacao   <- regex("NOMEAR|DESIGNAR|ADMITIR|CONTRATAR|PROVER", ignore_case = TRUE)
regex_exoneracao <- regex("EXONERAR|DISPENSAR|DEMITIR|DECLARAR VAGO", ignore_case = TRUE)

# 3. LISTAR ZIPS
zips <- list.files(DIR_ZIPS, pattern = "\\.zip$", full.names = TRUE)
if(length(zips) == 0) stop("❌ Nenhum ZIP encontrado! Rode o script 01 primeiro.")

print(paste("📦 Total de pacotes para processar:", length(zips)))

# 4. LOOP DE PROCESSAMENTO
lista_resumos <- list()

for(zip_file in zips) {
  nome_arquivo <- basename(zip_file)
  
  # Tenta extrair Ano/Mes do nome do arquivo (ex: dou_secao2_2002_Janeiro.zip)
  partes <- str_split(nome_arquivo, "_")[[1]]
  ano_ref <- partes[3]
  mes_nome <- str_remove(partes[4], "\\.zip")
  
  # Converte nome do mês para número
  mes_num <- match(mes_nome, c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                               "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"))
  
  data_base <- as.Date(paste(ano_ref, mes_num, "01", sep="-"))
  
  print(paste0("📂 Abrindo: ", mes_nome, "/", ano_ref))
  
  # Limpa temp
  file_delete(dir_ls(DIR_TEMP))
  
  tryCatch({
    unzip(zip_file, exdir = DIR_TEMP)
    xmls <- dir_ls(DIR_TEMP, glob = "*.xml")
    
    if(length(xmls) > 0) {
      # Leitura Rápida (Apenas texto para regex)
      # Nota: Usamos map_int para contar direto e economizar RAM
      contagem <- map_dfr(xmls, function(x) {
        txt <- tryCatch(xml_text(read_xml(x, options = "HUGE")), error = function(e) "")
        tibble(
          nom = str_detect(txt, regex_nomeacao),
          exo = str_detect(txt, regex_exoneracao)
        )
      })
      
      # Sumariza o mês inteiro
      resumo_mes <- tibble(
        Data = data_base,
        Nomeacoes = sum(contagem$nom, na.rm = TRUE),
        Exoneracoes = sum(contagem$exo, na.rm = TRUE),
        Total_Atos = nrow(contagem)
      )
      
      lista_resumos[[nome_arquivo]] <- resumo_mes
    }
  }, error = function(e) {
    print(paste("   ⚠️ Erro ao processar ZIP:", e$message))
  })
}

# 5. CONSOLIDAÇÃO FINAL
if(length(lista_resumos) > 0) {
  df_final <- bind_rows(lista_resumos) %>%
    arrange(Data) %>%
    mutate(
      Saldo = Nomeacoes - Exoneracoes,
      Ano = year(Data)
    )
  
  write_parquet(df_final, ARQUIVO_FINAL)
  print(paste("🎉 SUCESSO! Base salva em:", ARQUIVO_FINAL))
  print(df_final)
} else {
  print("❌ Nada foi processado.")
}