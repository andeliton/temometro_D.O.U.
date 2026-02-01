library(shiny)
library(bslib)
library(tidyverse)
library(arrow)
library(plotly)
library(waiter)
library(lubridate) # Essencial para extrair o Ano da Data

# ==============================================================================
# 0. DADOS GLOBAIS
# ==============================================================================
mandatos <- tibble(
  pres  = c("Lula I/II", "Dilma I/II", "Temer", "Bolsonaro", "Lula III"),
  start = as.Date(c("2002-01-01", "2011-01-01", "2016-08-31", "2019-01-01", "2023-01-01")),
  end   = as.Date(c("2010-12-31", "2016-08-30", "2018-12-31", "2022-12-31", "2026-12-31")),
  cor_hex  = c("#E1F5FE", "#E8F5E9", "#FFF3E0", "#FFEBEE", "#E1F5FE") 
)

# ==============================================================================
# UI
# ==============================================================================
ui <- page_sidebar(
  theme = bs_theme(version = 5, bootswatch = "flatly", primary = "#2c3e50"),
  
  # --- ROLAGEM FORÇADA (Importante para o layout funcionar) ---
  fillable = FALSE, 
  
  # CABEÇALHO
  title = div(
    style = "display: flex; align-items: center;", 
    img(src = "favicon_light.png", height = "45px", style = "margin-right: 15px;"), 
    div(
      div("Ars Metrica", style = "font-weight: 800; font-size: 1.1em; line-height: 1; color: #2c3e50;"),
      div("Termômetro Atos de Pessoal do Executivo Federal", style = "font-weight: 400; font-size: 0.8em; color: #7f8c8d;")
    )
  ),
  
  tags$head(
    tags$link(rel = "shortcut icon", href = "favicon_light.png"),
    tags$style(HTML("
      /* CSS GERAL */
      .navbar, .navbar-static-top, header.navbar { background-color: #FFFFFF !important; border-bottom: 1px solid #e0e0e0 !important; }
      .navbar-brand, .navbar-text { color: #2c3e50 !important; }
      
      /* --- KPI COMPACTO (SLIM) --- */
      .kpi-discreto {
        background-color: #FFFFFF !important;
        border: 1px solid #ecf0f1 !important;
        border-left: 4px solid #2c3e50 !important;
        box-shadow: none !important;
        border-radius: 4px !important;
        color: #2c3e50 !important;
        padding: 5px 10px !important; 
        min-height: 0 !important;
      }
      .kpi-discreto .value-box-value { font-size: 1.5rem !important; font-weight: 700; margin-bottom: 0 !important; line-height: 1.2; }
      .kpi-discreto .value-box-title { font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 1px; color: #7f8c8d !important; margin-bottom: 0 !important; }
      .kpi-discreto .value-box-showcase i { font-size: 1.8rem !important; opacity: 0.15; }
      
      /* Botões */
      .btn-download-custom { font-size: 0.8rem; padding: 2px 10px; background-color: #ecf0f1; color: #2c3e50; border: 1px solid #bdc3c7; }
      .btn-download-custom:hover { background-color: #bdc3c7; color: #fff; }
      
      /* Animação Loading (Pulsar) */
      @keyframes pulse { 0% { transform: scale(1); opacity: 1; } 50% { transform: scale(1.05); opacity: 0.8; } 100% { transform: scale(1); opacity: 1; } }
    "))
  ),
  
  use_waiter(), 
  
  # --- TELA DE CARREGAMENTO PERSONALIZADA ---
  waiter_show_on_load(
    html = tagList(
      # Logo pulsante (Se não tiver imagem, ele ignora sem quebrar)
      img(src = "favicon_light.png", height = "180px", style = "margin-bottom: 20px; animation: pulse 2s infinite; filter: drop-shadow(0px 5px 5px rgba(0,0,0,0.1));"),
      h4("Ars Metrica", style = "color: #2c3e50; font-family: sans-serif; font-weight: 600; letter-spacing: 2px;"),
      div("Carregando dados do Diário Oficial...", style = "color: #7f8c8d; font-size: 0.9em; margin-top: 10px;")
    ),
    color = "#FFFFFF" # Fundo branco limpo
  ),
  
  sidebar = sidebar(
    title = "Controle Temporal",
    uiOutput("aviso_arquivo"),
    sliderInput("range_anos", "Período:", min = 2002, max = 2026, value = c(2002, 2026), sep = "", step = 1),
    hr(),
    radioButtons("metrica", "Visualizar:", choices = c("Exonerações" = "exo", "Nomeações" = "nom", "Saldo Líquido" = "saldo")),
    hr(),
    div(style = "font-size: 0.8rem; color: #555; background-color: #f8f9fa; padding: 10px; border-radius: 4px; border-left: 4px solid #e74c3c;",
        icon("triangle-exclamation", style="color: #e74c3c; margin-right: 5px;"), 
        strong("Por que números tão altos?"), br(), br(),
        "Os dados refletem ", em("atos administrativos"), ", não necessariamente pessoas únicas.", br(), 
        "O volume inclui a rotatividade interna e renovação de contratos."
    ),
    div(class = "mt-auto", style = "padding-top: 20px; text-align: center;",
        hr(style = "margin: 10px 0; border-top: 1px solid #e0e0e0;"),
        div(style = "color: #7f8c8d; font-size: 0.8em; line-height: 1.4;",
            span("Powered by", style = "font-weight: 300;"), br(),
            span(icon("microchip"), " AUTOMATA", style = "font-family: monospace; font-weight: bold; color: #2c3e50; letter-spacing: 1px;"), br(),
            span("Ars Metrica Intelligence", style = "font-size: 0.85em; opacity: 0.8;")
        )
    )
  ),
  
  # --- CONTAINER PRINCIPAL (WRAPPER) ---
  div(
    class = "main-content-wrapper",
    style = "padding-bottom: 50px;", 
    
    # 1. KPIs (Compactos)
    layout_columns(
      fill = FALSE,
      value_box(title = "Volume Total (Período)", value = textOutput("kpi_total"), showcase = icon("file-lines"), class = "kpi-discreto"),
      tooltip(
        value_box(title = "Média Mensal", value = textOutput("kpi_media"), showcase = icon("chart-line"), class = "kpi-discreto"), 
        "Média de atos por mês."
      )
    ),
    
    br(),
    
    # 2. Card Linha do Tempo
    card(
      fill = FALSE, 
      card_header("Linha do Tempo", downloadButton("download_timeline", "Baixar Gráfico", class = "btn-download-custom")), 
      plotlyOutput("plot_timeline", height = "450px") 
    ),
    
    br(),
    
    # 3. Card Balanço Anual
    card(
      fill = FALSE, 
      style = "min-height: 400px; margin-bottom: 50px;", 
      card_header("Balanço Anual", downloadButton("download_balanco", "Baixar Gráfico", class = "btn-download-custom")),
      plotOutput("plot_annual_balance", height = "350px")
    )
  )
)

# ==============================================================================
# SERVER
# ==============================================================================
server <- function(input, output, session) {
  
  # 1. Carregamento
  dados_dou <- reactive({
    on.exit({ waiter_hide() }) 
    Sys.sleep(1.0) 
    
    arquivo <- "base_dou_dashboard.parquet"
    if (!file.exists(arquivo)) return(NULL)
    
    tryCatch({
      read_parquet(arquivo) %>%
        mutate(
          Data = as.Date(Data),
          # --- AQUI ESTAVA O ERRO ---
          # Criamos o Ano a partir da Data, pois a coluna 'Ano' não existe no arquivo original
          Ano = year(Data), 
          Exoneracoes = replace_na(Exoneracoes, 0), 
          Nomeacoes = replace_na(Nomeacoes, 0)
        )
    }, error = function(e) return(NULL))
  })
  
  output$aviso_arquivo <- renderUI({
    if (is.null(dados_dou())) div(class = "alert alert-danger", "ERRO: Arquivo .parquet não encontrado.") else NULL
  })
  
  dados_filtrados <- reactive({
    req(dados_dou())
    dados_dou() %>% filter(Ano >= input$range_anos[1] & Ano <= input$range_anos[2])
  })
  
  output$kpi_total <- renderText({
    df <- dados_filtrados(); if(nrow(df)==0) return("0")
    format(sum(df$Exoneracoes+df$Nomeacoes), big.mark=".", decimal.mark=",")
  })
  
  output$kpi_media <- renderText({
    df <- dados_filtrados(); if(nrow(df)==0) return("0")
    format(round(mean(df$Exoneracoes+df$Nomeacoes, na.rm=TRUE),0), big.mark=".", decimal.mark=",")
  })
  
  # ----------------------------------------------------------------------------
  # TELA: Linha do Tempo
  # ----------------------------------------------------------------------------
  output$plot_timeline <- renderPlotly({
    df <- dados_filtrados()
    req(nrow(df) > 0)
    
    df_plot <- df %>% arrange(Data) %>%
      mutate(Valor = case_when(input$metrica == "exo" ~ Exoneracoes, input$metrica == "nom" ~ Nomeacoes, input$metrica == "saldo" ~ Nomeacoes-Exoneracoes, TRUE ~ Nomeacoes+Exoneracoes),
             Pres = case_when(Data<"2011-01-01"~"Lula I/II", Data<"2016-08-31"~"Dilma", Data<"2019-01-01"~"Temer", Data<"2023-01-01"~"Bolsonaro", TRUE~"Lula III"),
             Txt = paste0("<b>Presidente:</b> ", Pres, "<br><b>Data:</b> ", format(Data, "%m/%Y"), "<br><b>Atos:</b> ", format(Valor, big.mark=".")))
    
    cor <- case_when(input$metrica=="exo"~"#c0392b", input$metrica=="nom"~"#27ae60", input$metrica=="saldo"~"#2980b9", TRUE~"#8e44ad")
    
    max_y <- max(df_plot$Valor, na.rm = TRUE)
    min_y <- min(df_plot$Valor, na.rm = TRUE)
    topo_fundo <- if(max_y > 0) max_y * 1.2 else max_y * 0.8
    base_fundo <- if(min_y < 0) min_y * 1.2 else 0
    
    p <- ggplot() +
      geom_line(data=df_plot, aes(x=Data, y=Valor), alpha=0) +
      geom_rect(data=mandatos, aes(xmin=start, xmax=end, ymin=base_fundo, ymax=topo_fundo, fill=cor_hex), alpha=0.6, inherit.aes = FALSE) + 
      scale_fill_identity() +
      geom_area(data=df_plot, aes(x=Data, y=Valor), fill=cor, alpha=0.3) +
      geom_line(data=df_plot, aes(x=Data, y=Valor), color=cor, linewidth=0.7) +
      geom_point(data=df_plot, aes(x=Data, y=Valor, text=Txt), alpha=0) +
      theme_minimal() + 
      labs(x=NULL, y=NULL)
    
    ggplotly(p, tooltip="text") %>% 
      layout(showlegend=FALSE) %>% 
      config(displayModeBar = TRUE, modeBarButtonsToRemove = c("toImage", "zoomIn2d", "zoomOut2d", "pan2d", "select2d", "lasso2d")) 
  })
  
  # ----------------------------------------------------------------------------
  # TELA: Balanço Anual
  # ----------------------------------------------------------------------------
  output$plot_annual_balance <- renderPlot({
    df <- dados_filtrados()
    if(nrow(df) == 0) return(NULL)
    
    df %>% 
      group_by(Ano) %>% 
      summarise(S=sum(Nomeacoes-Exoneracoes)) %>% 
      mutate(St=ifelse(S>=0,"Expansão","Retração")) %>%
      ggplot(aes(x=factor(Ano), y=S, fill=St)) + 
      geom_col(alpha=0.85) + 
      scale_fill_manual(values=c("Expansão"="#27ae60","Retração"="#c0392b")) +
      geom_hline(yintercept=0) + 
      theme_minimal() + 
      theme(axis.text.x=element_text(angle=45, hjust=1), legend.position="top") + 
      labs(x=NULL, y="Saldo", fill=NULL)
  })
  
  # ----------------------------------------------------------------------------
  # DOWNLOADS
  # ----------------------------------------------------------------------------
  output$download_timeline <- downloadHandler(
    filename = function() { paste("ars_metrica_timeline_", Sys.Date(), ".png", sep = "") },
    content = function(file) {
      df <- dados_filtrados()
      df_plot <- df %>% arrange(Data) %>%
        mutate(Valor = case_when(input$metrica == "exo" ~ Exoneracoes, input$metrica == "nom" ~ Nomeacoes, input$metrica == "saldo" ~ Nomeacoes-Exoneracoes, TRUE ~ Nomeacoes+Exoneracoes))
      cor <- case_when(input$metrica=="exo"~"#c0392b", input$metrica=="nom"~"#27ae60", input$metrica=="saldo"~"#2980b9", TRUE~"#8e44ad")
      tit <- case_when(input$metrica == "exo" ~ "Histórico de Exonerações", input$metrica == "nom" ~ "Histórico de Nomeações", TRUE ~ "Saldo Líquido")
      
      p_arquivo <- ggplot() +
        geom_rect(data=mandatos, aes(xmin=start, xmax=end, ymin=-Inf, ymax=Inf, fill=cor_hex), alpha=0.6, inherit.aes = FALSE) + 
        scale_fill_identity() +
        geom_area(data=df_plot, aes(x=Data, y=Valor), fill=cor, alpha=0.3) +
        geom_line(data=df_plot, aes(x=Data, y=Valor), color=cor, linewidth=0.7) +
        theme_minimal() +
        labs(title = tit, caption = "Fonte: Ars Metrica | Powered by Automata", x=NULL, y=NULL) +
        theme(plot.title = element_text(face = "bold", size = 14, color = "#2c3e50"), plot.caption = element_text(family = "mono", color = "#7f8c8d", size = 10, margin = margin(t = 15)))
      ggsave(file, plot = p_arquivo, device = "png", width = 10, height = 6, dpi = 300)
    }
  )
  
  output$download_balanco <- downloadHandler(
    filename = function() { paste("ars_metrica_balanco_", Sys.Date(), ".png", sep = "") },
    content = function(file) {
      df <- dados_filtrados()
      tit <- paste("Balanço Anual:", input$range_anos[1], "-", input$range_anos[2])
      p_final <- df %>% group_by(Ano) %>% summarise(S=sum(Nomeacoes-Exoneracoes)) %>% mutate(St=ifelse(S>=0,"Expansão","Retração")) %>%
        ggplot(aes(x=factor(Ano), y=S, fill=St)) + geom_col(alpha=0.85) + scale_fill_manual(values=c("Expansão"="#27ae60","Retração"="#c0392b")) +
        geom_hline(yintercept=0) + theme_minimal() + 
        labs(title = tit, caption = "Fonte: Ars Metrica | Powered by Automata", x=NULL, y="Saldo", fill=NULL) +
        theme(axis.text.x=element_text(angle=45, hjust=1), legend.position="top", plot.title = element_text(face = "bold", size = 14, color = "#2c3e50"), plot.caption = element_text(family = "mono", color = "#7f8c8d", size = 10, margin = margin(t = 15)))
      ggsave(file, plot = p_final, device = "png", width = 10, height = 6, dpi = 300)
    }
  )
}

shinyApp(ui, server)