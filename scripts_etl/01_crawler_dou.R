# ==============================================================================
# ETL 01: CRAWLER DO DIÁRIO OFICIAL (DADOS ABERTOS) - VERSÃO SEGURA
# ==============================================================================
# Objetivo: 
# 1. Navegar nas páginas mensais do in.gov.br.
# 2. Localizar o link dinâmico do ZIP da "Seção 2" (Pessoal).
# 3. Baixar o arquivo com verificação de integridade (tamanho).
# ==============================================================================

library(tidyverse)
library(rvest)  # Para ler o HTML
library(httr)   # Para baixar os arquivos

print("🕷️ INICIANDO CRAWLER DO DOU (COM PROTEÇÃO CONTRA FALHAS)...")

# 1. CONFIGURAÇÕES
# Defina o intervalo de anos que você quer baixar
ANOS_ALVO   <- 2002:2024  
DIR_DESTINO <- "dados_zips_download"

if(!dir.exists(DIR_DESTINO)) dir.create(DIR_DESTINO)

# Lista de meses (Grafia exata do site)
MESES <- c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
           "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")

# 2. FUNÇÃO DE RASPAGEM SEGURA
baixar_mes_dou <- function(ano, mes) {
  
  nome_arquivo <- paste0("dou_secao2_", ano, "_", mes, ".zip")
  caminho_final <- file.path(DIR_DESTINO, nome_arquivo)
  
  # --- TRAVA DE SEGURANÇA (Cache Inteligente) ---
  # Só pula o download se:
  # 1. O arquivo existe
  # 2. E o arquivo tem mais de 1KB (evita arquivos vazios/corrompidos de quedas de net)
  if(file.exists(caminho_final)) {
    info <- file.info(caminho_final)
    if(!is.na(info$size) && info$size > 1000) {
      print(paste("   ⏭️ Já baixado e válido (Cache):", nome_arquivo))
      return(NULL)
    } else {
      print(paste("   ⚠️ Arquivo corrompido/vazio encontrado. Baixando novamente:", nome_arquivo))
    }
  }
  
  # Monta URL da página de navegação
  url_pagina <- paste0(
    "https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados",
    "?ano=", ano, "&mes=", mes
  )
  
  print(paste("🔎 Buscando link para:", mes, "/", ano, "..."))
  
  tryCatch({
    # Acessa a página com User-Agent para não ser bloqueado
    u_agent <- user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    sessao <- GET(url_pagina, u_agent)
    
    # Lê o HTML retornado
    pagina <- read_html(content(sessao, as = "text"))
    
    # Procura todos os links
    links <- pagina %>% 
      html_nodes("a") %>% 
      html_attr("href")
    
    # Filtra: Queremos ZIPs que tenham "S02" (Seção 2) no nome ou link
    # O regex procura "S02" seguido de qualquer coisa até ".zip"
    link_zip <- links[str_detect(links, "S02.*\\.zip") & !is.na(links)]
    
    if(length(link_zip) == 0) {
      # Tenta uma busca mais genérica se falhar (alguns anos antigos mudam o padrão)
      link_zip <- links[str_detect(links, "secao-2.*\\.zip") & !is.na(links)]
    }
    
    if(length(link_zip) == 0) {
      warning("   ⚠️ Link da Seção 2 não encontrado nesta página. Pulando.")
      return(NULL)
    }
    
    # Arruma o link (alguns vêm relativos, outros absolutos)
    link_final <- link_zip[1]
    if(!str_starts(link_final, "http")) {
      link_final <- paste0("https://in.gov.br", link_final)
    }
    
    # --- DOWNLOAD ---
    print("   ⬇️ Baixando arquivo ZIP...")
    
    # timeout(600) dá 10 minutos por arquivo (para conexões lentas)
    GET(link_final, u_agent, write_disk(caminho_final, overwrite = TRUE), timeout(600))
    
    # Verificação pós-download
    info_pos <- file.info(caminho_final)
    if(info_pos$size < 1000) {
      warning("   ❌ Download parece ter falhado (arquivo vazio). Será tentado na próxima vez.")
      file.remove(caminho_final) # Apaga para não atrapalhar
    } else {
      print("   ✅ Sucesso!")
    }
    
    # Pausa ética (Evita bloqueio de IP pelo servidor do governo)
    Sys.sleep(runif(1, 2, 5))
    
  }, error = function(e) {
    print(paste("   ❌ Erro de conexão/processamento:", e$message))
  })
}

# 3. LOOP DE EXECUÇÃO
# O 'walk' é como um 'for', mas mais limpo.
print("🚀 Iniciando varredura...")

for(ano in ANOS_ALVO) {
  print(paste("📅 --- PROCESSANDO ANO:", ano, "---"))
  # Percorre todos os meses daquele ano
  walk(MESES, ~baixar_mes_dou(ano, .x))
}

print("🏁 Crawler finalizado. Verifique a pasta 'dados_zips_download'.")