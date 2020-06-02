#Author: Yannick Schaper

#Description: 
#See the Hands-On Tutorial: Leverage SAP HANA embedded Machine Learning through an R Shiny App 
#
#Requirements: 
#- ngdbc.jar file
#- hana.ml.r package version 1.0.8
#- SAP HANA 2 SPS04 
#- R Version: 3.6.3
#
#Output
#- HANA table containing the results
#- PAL K-means model
#- Different visualizations
#- Shiny App

#---------------------------------------------------------------------------------------------------------------------------------------------------

#Set Java Home
Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre1.8.0_241')

#Installing and loading required packages
install.packages("R6")
install.packages("sets")
install.packages("rJava")
install.packages("RJDBC")
install.packages("futile.logger")
install.packages("uuid")
install.packages("shiny")
install.packages("ggplot2")
install.packages("hana.ml.r_1.0.8.tar.gz", repos=NULL, type="source")
library(R6)
library(sets)
library(rJava)
library(RJDBC)
library(futile.logger)
library(uuid)
library(shiny)
library(ggplot2)
library(hana.ml.r)

#--------------------------------------------------------------------------------------------------------------------------------------------------

#create connection through hana.ml.r package (SAP)
conn_jdbc <- hanaml.ConnectionContext(dsn,
                                      username,
                                      password,
                                      odbc=FALSE, 
                                      jdbcDriver = "C:/ngdbc.jar") 


#create connection through RJDBC package (CRAN)
jdbcDriver <- JDBC(driverClass="com.sap.db.jdbc.Driver",  
                   classPath="C:/ngdbc.jar")

jdbcConnection <- dbConnect(jdbcDriver,
                            'jdbc:sap://<dsn>',
                            "username",
                            'password')

#--------------------------------------------------------------------------------------------------------------------------------------------------

#simulate data
set.seed(29)
x=matrix(rnorm(100*2),100,2)
xmean=matrix(rnorm(16,sd=4),8,2)
which=sample(1:4,100,replace=TRUE)
x=x+xmean[which,]
x <- data.frame(x)
x <- data.frame(Y = x$X1, X = x$X2, WHICH = which)

#push data into SAP HANA
dbWriteTable(jdbcConnection, "KMEANSDEMO",x, overwrite = TRUE)

#load data directly from SAP HANA 
data<- conn$table(table = "KMEANSDEMO",
                  schema = "RUSER")

data <- conn$sql(conn$sql("SELECT * FROM RUSER.KMEANSDEMO"))

#load help page of K-means PAL
?hanaml.Kmeans

#add ID to the dataset
data <- data$AddId("ID")

#drop the variable which from the data set
data <- data$Drop("WHICH")

#apply K-Means algorithm from PAL
km <- hanaml.Kmeans(conn.context = conn,
                    data = data,
                    key = "ID",
                    n.clusters = 4,
                    features = NULL)


#load whole data set again
data<- conn$table(table = "KMEANSDEMO",
                  schema = "RUSER")

#collect data from SAP HANA
collect <- data$Collect()

#collect the results from the KMeans algorithm
cluster <- km$labels$Collect()+1

#combine data
total <- data.frame(Y = collect$Y,X = collect$X, cluster, which = collect$WHICH)

#plot the results
km_plot <- ggplot(data = total, aes(Y, X))
km_plot + geom_point(aes(colour = as.factor(which)), size = 3)+
  geom_point(aes(colour = as.factor(CLUSTER_ID)), shape=1, size=5, stroke=1.5)+
  scale_color_brewer(palette="Paired") +
  theme(panel.grid = element_line(linetype = "dashed", color = "lightgrey"), 
        panel.background = element_rect(fill = "white"),
        panel.border = element_rect(colour = "black", fill=NA),
        plot.title = element_text(hjust = 0.5),
        axis.title.x = element_text(size = 16),
        axis.title.y = element_text(size = 16),
        axis.text.x = element_text(size = 16),
        axis.text.y = element_text(size = 16),
        legend.title = element_blank()) + 
  ylab("Y")+
  xlab("X")

#push the results into SAP HANA
dbWriteTable(jdbcConnection, "KMEANSRESULTS",total, overwrite = TRUE)

#save model into SAP HANA
km$model$save("KMEANSMODELRESULTS")

#--------------------------------------------------------------------------------------------------------------------------------------------------

#Create shiny app 

# Define UI for app
ui <- fluidPage(
  
  # App title
  titlePanel("R & SAP HANA"),
  
  # Sidebar layout with input and output definitions
  sidebarLayout(
    
    # Sidebar panel for inputs
    sidebarPanel(
      
      # Input: Slider for the number of clusters
      sliderInput(inputId = "clusters",
                  label = "Number of clusters:",
                  min = 2,
                  max = 10,
                  value = 4)
      
    ),
    
    # Main panel for displaying outputs
    mainPanel(
      
      # Output: Plot of clusters
      plotOutput(outputId = "distPlot")
      
    )
  )
)

# Define server logic required to compute K-means and plot results
server <- function(input, output) {
  output$distPlot <- renderPlot({
    
    #connect to SAP HANA
    library(hana.ml.r)
    conn <- hanaml.ConnectionContext('dsn',"user",'password', odbc=FALSE, 
                                          jdbcDriver = "C:/ngdbc.jar") 
    
    #load the data from SAP HANA
    data <- conn$table(table = "KMEANSDEMO",
                       schema = "RUSER")
    
    #Add ID as key
    data <- data$AddId("ID")
    
    #capture number of clusters
    number <- as.numeric(input$clusters)
    
    #compute K-means algorithm (PAL)
    km <- hanaml.Kmeans(conn.context = conn,
                        data = data,
                        key = "ID",
                        n.clusters = number,
                        features = NULL)
    
    #collect the results from the KMeans algorithm
    cluster <- km$labels$Collect()+1
    
    #combine data
    total <- data.frame(Y = collect$Y,X = collect$X, cluster, which = collect$WHICH)
    
    #plot the results
    km_plot <- ggplot(data = total, aes(Y, X))
    km_plot + geom_point(aes(colour = as.factor(which)), size = 3)+
      geom_point(aes(colour = as.factor(CLUSTER_ID)), shape=1, size=5, stroke=1.5)+
      scale_color_brewer(palette="Paired") +
      theme(panel.grid = element_line(linetype = "dashed", color = "lightgrey"), 
            panel.background = element_rect(fill = "white"),
            panel.border = element_rect(colour = "black", fill=NA),
            plot.title = element_text(hjust = 0.5),
            axis.title.x = element_text(size = 16),
            axis.title.y = element_text(size = 16),
            axis.text.x = element_text(size = 16),
            axis.text.y = element_text(size = 16),
            legend.title = element_blank()) + 
      ylab("Y")+
      xlab("X")
  })
}

#create the shiny app
shinyApp(ui = ui, server = server)

#--------------------------------------------------------------------------------------------------------------------------------------------------













