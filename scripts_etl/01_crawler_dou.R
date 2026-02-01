# ==============================================================================
# ETL 01: CRAWLER DO DIÁRIO OFICIAL - CORREÇÃO 2024
# ==============================================================================
library(tidyverse)
library(rvest)
library(httr)

print("🕷️ INICIANDO CRAWLER DO DOU (VERSÃO CORRIGIDA 2024)...")

# 1. CONFIGURAÇÕES
# Focando a atualização para garantir 2024, mas mantendo o histórico
ANOS_ALVO   <- 2002:2024  
DIR_DESTINO <- "dados_zips_download"

if(!dir.exists(DIR_DESTINO)) dir.create(DIR_DESTINO)

MESES <- c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
           "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")

# 2. FUNÇÃO DE RASPAGEM SEGURA
baixar_mes_dou <- function(ano, mes) {
  
  # Nome padronizado para salvar no nosso computador
  nome_arquivo <- paste0("dou_secao2_", ano, "_", mes, ".zip")
  caminho_final <- file.path(DIR_DESTINO, nome_arquivo)
  
  # Cache: Se já baixou e o arquivo é grande (>1KB), pula
  if(file.exists(caminho_final)) {
    info <- file.info(caminho_final)
    if(!is.na(info$size) && info$size > 1000) {
      print(paste("   ⏭️ Já baixado (Cache):", nome_arquivo))
      return(NULL)
    }
  }
  
  url_pagina <- paste0("https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados?ano=", ano, "&mes=", mes)
  
  print(paste("🔎 Buscando:", mes, "/", ano))
  
  tryCatch({
    u_agent <- user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    sessao <- GET(url_pagina, u_agent)
    pagina <- read_html(content(sessao, as = "text"))
    
    links <- pagina %>% html_nodes("a") %>% html_attr("href")
    
    # --- CORREÇÃO DE REGEX PARA 2024 ---
    # Prioriza links que contenham explicitamente "secao-2" ou "s2" ou "do2"
    # E evita "secao-1" ou "secao-3"
    
    # 1. Tenta o padrão mais comum (S02)
    link_zip <- links[str_detect(links, "(?i)(S02|secao-2|do2).*\\.zip") & !is.na(links)]
    
    # 2. Filtro de segurança: Remove Seção 1 e 3 se vierem por engano no regex
    link_zip <- link_zip[!str_detect(link_zip, "(?i)(S01|S03|secao-1|secao-3|do1|do3)")]
    
    if(length(link_zip) == 0) {
      warning("   ⚠️ Link da Seção 2 não encontrado.")
      return(NULL)
    }
    
    # Pega o primeiro link válido encontrado
    link_final <- link_zip[1]
    if(!str_starts(link_final, "http")) {
      link_final <- paste0("https://in.gov.br", link_final)
    }
    
    print("   ⬇️ Baixando...")
    GET(link_final, u_agent, write_disk(caminho_final, overwrite = TRUE), timeout(600))
    
    # Validação final
    if(file.info(caminho_final)$size < 1000) {
      warning("   ❌ Arquivo vazio. Deletando.")
      file.remove(caminho_final)
    } else {
      print("   ✅ Sucesso!")
    }
    
    Sys.sleep(2) # Pausa ética
    
  }, error = function(e) {
    print(paste("   ❌ Erro:", e$message))
  })
}

# 3. EXECUÇÃO
for(ano in ANOS_ALVO) {
  print(paste("📅 --- PROCESSANDO ANO:", ano, "---"))
  walk(MESES, ~baixar_mes_dou(ano, .x))
}