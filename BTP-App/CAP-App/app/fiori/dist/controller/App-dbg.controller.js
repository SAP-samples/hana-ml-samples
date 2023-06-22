sap.ui.define(
  [
    "sap/ui/core/mvc/Controller"
  ],
  function (BaseController) {
    "use strict";

    return BaseController.extend("cap.ui.fiori.controller.App", {
      onInit: function () {
        this.oRouter = this.getOwnerComponent().getRouter();
        this.oRouter.attachRoutePatternMatched(this.onRoutePatternMatched.bind(this));
      },

      onRoutePatternMatched: function (oEvent) {
        this._updateFlexLayoutStates(oEvent.getParameter("name"));
      },

      _updateFlexLayoutStates: function (sRoute) {
        if (sRoute === "RoutePoS_Main") this._setLayout("OneColumn");
        else if (sRoute === "RoutePoS_Detail") this._setLayout("TwoColumnsMidExpanded");
      },

      _setLayout: function(sLayout) {
        var oModel = this.getOwnerComponent().getModel("global");
        oModel.setProperty("/layout", sLayout);
      }

    });
  }
);
