const cds = require('@sap/cds');

module.exports = cds.service.impl(async function () {
    this.on("Prices_Predict", async (req) => {
        try {
            const dbClass = require("sap-hdb-promisfied")

            let db = new dbClass(await dbClass.createConnectionFromEnv())
            let dbProcQuery = "CALL HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_PREDICT(OUT_0_HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_PREDICT => ?,OUT_1_HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_PREDICT => ?,OUT_2_HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_PREDICT => ?)"


            console.log("------Before running db procedure--------")
            let result = await db.execSQL(dbProcQuery)

            console.log("------After running db procedure--------")
            console.table(result)
            return true
        } catch (error) {
            console.error(error)
            return false
        }
    });

    this.on("Model_Train", async (req) => {
        try {
            const dbClass = require("sap-hdb-promisfied")

            let db = new dbClass(await dbClass.createConnectionFromEnv())
            let dbProcQuery = "CALL HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_ANALYSIS(OUT_0_HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_ANALYSIS => ?,OUT_1_HANA_ML_CONS_PAL_MASSIVE_ADDITIVE_MODEL_ANALYSIS => ?)"

            console.log("------Before running db procedure--------")
            let result = await db.execSQL(dbProcQuery)

            console.log("------After running db procedure--------")
            console.table(result)
            return true
        } catch (error) {
            console.error(error)
            return false
        }
    });
});