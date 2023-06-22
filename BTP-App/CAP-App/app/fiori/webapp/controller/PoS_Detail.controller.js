sap.ui.define([
    "sap/ui/core/mvc/Controller",
    "sap/ui/model/Filter",
    "sap/ui/model/FilterOperator",
    "sap/ui/model/json/JSONModel",
    'sap/viz/ui5/controls/common/feeds/FeedItem'
],
    /**
     * @param {typeof sap.ui.core.mvc.Controller} Controller
     */
    function (Controller, Filter, FilterOperator, JSONModel, FeedItem) {
        "use strict";

        return Controller.extend("cap.ui.fiori.controller.PoS_Detail", {
            onInit: function () {
                var oChartModel = new JSONModel();
                this.getView().setModel(oChartModel, "oChartModel");
                var oPredictModel = new JSONModel();
                this.getView().setModel(oPredictModel, "oPredictModel");
                // var oTrainModel = new JSONModel();
                // this.getView().setModel(oTrainModel, "oTrainModel");
                this.oRouter = this.getOwnerComponent().getRouter()
                this.oRouter.getRoute("RoutePoS_Detail").attachPatternMatched(this._onObjectMatched, this)
            },

            _onObjectMatched: async function (oEvent) {
                this._sPoSid = oEvent.getParameter("arguments").pointofsale;
                await this._bindView(this._sPoSid);

                this.setVizChartProperties();
                this.fetchChartData(this._sPoSid);

                this.fetchPredictData(this._sPoSid);
                // this.fetchTrainData(this._sPoSid);
            },

            _bindView: function(sPoSid) {
                return new Promise(function (resolve) {
                    this.getView().bindElement({
                        path: "/POINTS_OF_SALES/" + sPoSid,
                        events: {
                            change: resolve
                        },
                        refreshAfterChange: true
                    });
                }.bind(this));
            },

            onCloseFlexibleColumn: function () {
                this.oRouter.navTo("RoutePoS_Main");
            },

            setVizChartProperties: function () {
                var oVizFrame = this.getView().byId("priceOverTimeVizFrame");
                oVizFrame.setVizProperties({
                    title: { visible: false },
                    plotArea: {
                        dataLabel: {
                            visible: false
                        },
                        window: {
                            start: 'firstDataPoint',
                            end: 'lastDataPoint'
                        },
                        primaryScale: {
                            fixedRange: true,
                            minValue: 1.6,
                            maxValue: 2.4
                        }
                    }
                });
            },

            fetchChartData: function(sPoSid){
                this.getView().getModel().read("/History_Forecast", {
                    filters: [new Filter("uuid", FilterOperator.EQ, sPoSid)],
                    success: function (oResponse) {
                        this.getView().getModel("oChartModel").setProperty("/", oResponse.results);
                    }.bind(this),
                    error: function (oError) {
                        console.log(oError);
                    }
                });
            },

            onAfterRendering: function () {
                const oVizFrame = this.getView().byId("priceOverTimeVizFrame");
                const oPopOver = this.getView().byId("idPopOver");
                oPopOver.connect(oVizFrame.getVizUid());
            },

            displayPredictedData: function () {
                const oViz = this.getView().byId("priceOverTimeVizFrame")
                if (oViz.getFeeds().length >= 3) {
                    oViz.removeFeed(2)
                } else {
                    oViz.addFeed(new FeedItem(
                        {
                            "uid": "valueAxis",
                            "type": "Measure",
                            "values": ["Predicted Price"]
                        }
                    ))
                }
            },

            fetchPredictData: function (sPoSid){
                this.getView().getModel().read("/ModelHanaMlConsPalMassiveAdditiveModelAnalysis", {
                    filters: [new Filter("group_id", FilterOperator.EQ, sPoSid)],
                    success: function (oResponse) {
                        if(oResponse.results.length > 0) this.getView().getModel("oPredictModel").setProperty("/", JSON.parse(oResponse.results[0].model_content));
                    }.bind(this),
                    error: function (oError) {
                        console.log(oError);
                    }
                });
            },

            // fetchTrainData: function(sPoSid){
            //     this.getView().getModel().read("/Output1PalMassiveAdditiveModelPredict", {
            //         filters: [new Filter("group_id", FilterOperator.EQ, sPoSid)],
            //         success: function (oResponse) {
            //             debugger;
            //             // if(oResponse.results.length > 0) this.getView().getModel("oTrainModel").setProperty("/", JSON.parse(oResponse.results[0].model_content));
            //         }.bind(this),
            //         error: function (oError) {
            //             console.log(oError);
            //         }
            //     });
            // }
        });
    });
