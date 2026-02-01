# ==============================================================================
# ETL 02: PROCESSADOR DE XML (COM DESDUPLICAÇÃO MD5)
# ==============================================================================
library(tidyverse)
library(xml2)
library(arrow)
library(fs)
library(lubridate)
library(tools) # Necessário para o md5sum

print("⚙️ INICIANDO PROCESSAMENTO (COM PROTEÇÃO ANTI-DUPLICIDADE)...")

# 1. CONFIGURAÇÃO
DIR_ZIPS      <- "dados_zips_download"
DIR_TEMP      <- "temp_xml_extract"
ARQUIVO_FINAL <- "base_dou_dashboard.parquet"

if(!dir.exists(DIR_TEMP)) dir.create(DIR_TEMP)

# 2. REGEX (Mantendo o padrão que funcionou)
regex_nomeacao   <- regex("NOMEAR|DESIGNAR|ADMITIR|CONTRATAR|PROVER", ignore_case = TRUE)
regex_exoneracao <- regex("EXONERAR|DISPENSAR|DEMITIR|DECLARAR VAGO", ignore_case = TRUE)

# 3. LISTAR ZIPS
zips <- list.files(DIR_ZIPS, pattern = "\\.zip$", full.names = TRUE)
if(length(zips) == 0) stop("❌ Nenhum ZIP encontrado!")

# 4. LOOP DE PROCESSAMENTO
lista_resumos <- list()

for(zip_file in zips) {
  nome_arquivo <- basename(zip_file)
  
  # Extração da data do nome do arquivo
  partes <- str_split(nome_arquivo, "_")[[1]]
  ano_ref <- partes[3]
  mes_nome <- str_remove(partes[4], "\\.zip")
  mes_num <- match(mes_nome, c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                               "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"))
  
  data_base <- as.Date(paste(ano_ref, mes_num, "01", sep="-"))
  
  print(paste0("📂 Processando: ", mes_nome, "/", ano_ref))
  
  # Limpa pasta temporária
  file_delete(dir_ls(DIR_TEMP))
  
  tryCatch({
    unzip(zip_file, exdir = DIR_TEMP)
    
    # Lista todos os XMLs extraídos
    xmls <- dir_ls(DIR_TEMP, glob = "*.xml", recurse = TRUE)
    
    if(length(xmls) > 0) {
      
      # --- CORREÇÃO CRÍTICA PARA 2024: DESDUPLICAÇÃO ---
      # Calcula o 'DNA' (MD5) de cada arquivo. 
      # Se houver arquivos idênticos com nomes diferentes, o MD5 será igual.
      hashes <- tools::md5sum(xmls)
      
      # Filtra apenas os arquivos únicos baseados no conteúdo
      xmls_unicos <- xmls[!duplicated(hashes)]
      
      if(length(xmls) != length(xmls_unicos)) {
        print(paste("   ⚠️  Duplicatas detectadas e removidas:", length(xmls) - length(xmls_unicos)))
      }
      
      # Processa apenas os únicos
      contagem <- map_dfr(xmls_unicos, function(x) {
        # Lê o XML como texto bruto (mais rápido e robusto a erros de formatação)
        txt <- tryCatch(xml_text(read_xml(x, options = "HUGE")), error = function(e) "")
        
        tibble(
          nom = str_detect(txt, regex_nomeacao),
          exo = str_detect(txt, regex_exoneracao)
        )
      })
      
      # Consolida o mês
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

# 5. SALVAMENTO FINAL
if(length(lista_resumos) > 0) {
  df_final <- bind_rows(lista_resumos) %>%
    arrange(Data) %>%
    mutate(
      Saldo = Nomeacoes - Exoneracoes,
      Ano = year(Data)
    )
  
  # Salva a base corrigida
  write_parquet(df_final, ARQUIVO_FINAL)
  
  print("✅ BASE ATUALIZADA COM SUCESSO!")
  print(paste("   Arquivo salvo em:", ARQUIVO_FINAL))
  
  # Mostra uma prévia de 2024 para confirmar a correção
  print("--- Verificação 2024 ---")
  print(df_final %>% filter(Ano == 2024))
  
} else {
  print("❌ Erro: Nenhum dado processado.")
}