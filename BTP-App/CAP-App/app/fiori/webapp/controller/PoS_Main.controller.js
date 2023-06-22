sap.ui.define([
    "sap/ui/core/mvc/Controller",
    "sap/m/MessageToast"
],
    /**
     * @param {typeof sap.ui.core.mvc.Controller} Controller
     */
    function (Controller, MessageToast) {
        "use strict";

        return Controller.extend("cap.ui.fiori.controller.PoS_Main", {
            onInit: function () {
                this.oRouter = this.getOwnerComponent().getRouter();
            },

            onListItemPress: function (oEvent) {
                var sPoSId = oEvent.getSource().getSelectedItem().getBindingContext().getProperty("uuid");
                this.oRouter.navTo("RoutePoS_Detail", { pointofsale: sPoSId });
            },

            onPredict: async function () {
                await this._callFI("Prices_Predict");
                this._setBusyView(false);
            },

            onTrain: async function () {
                await this._callFI("Model_Train");
                this._setBusyView(false);
            },

            _setBusyView: function (bBusy) {
                this.getView().setBusy(bBusy);
            },

            _callFI: function (sFI) {
                return new Promise(function (resolve) {
                    this._setBusyView(true);
                    this.getView().getModel().callFunction("/" + sFI, {
                        success: function (oResponse) {
                            var oi18n = this.getView().getModel("i18n").getResourceBundle();
                            if(oResponse[sFI]) MessageToast.show(oi18n.getText("FiSucc",[oi18n.getText(sFI)]));
                            else MessageToast.show(oi18n.getText("FiErr",[oi18n.getText(sFI)]));
                            resolve();
                        }.bind(this)
                    })
                }.bind(this));
            }
        });
    });
