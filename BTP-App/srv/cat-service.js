const cds = require('@sap/cds')
module.exports = cds.service.impl(function () {
    this.on('Prices_Predict', async () => {
    try {
        const dbClass = require("sap-hdb-promisfied")
         /*let dbConn = new dbClass(await dbClass.createConnectionFromEnv())
        const autoarima = await dbConn.loadProcedurePromisified(null, '"CONS_BASE_AUTOARIMA1_FIT"')
        const output = await dbConn.callProcedurePromisified(autoarima, [])
        console.log(output.results)*/
        let db = new dbClass(await dbClass.createConnectionFromEnv())
        let dbProcQuery = "CALL CONS_PREDICT()"        
        // @ts-ignore - CDS Types aren't updated for this new Stored Procedure option yet 
        console.log("------Before running db procedure--------")
        let result = await db.execSQL(dbProcQuery)

        console.log("------After running db procedure--------")
        console.table(result)
        return true
    } catch (error) {
        console.error(error)
        return false
    }
    })

    this.on('Model_Train', async () => {
        try {
            const dbClass = require("sap-hdb-promisfied")
             /*let dbConn = new dbClass(await dbClass.createConnectionFromEnv())
            const autoarima = await dbConn.loadProcedurePromisified(null, '"CONS_BASE_AUTOARIMA1_FIT"')
            const output = await dbConn.callProcedurePromisified(autoarima, [])
            console.log(output.results)*/
            let db = new dbClass(await dbClass.createConnectionFromEnv())
            let dbProcQuery = "CALL CONS_TRAIN()"        
            // @ts-ignore - CDS Types aren't updated for this new Stored Procedure option yet 
            console.log("------Before running db procedure--------")
            let result = await db.execSQL(dbProcQuery)
    
            console.log("------After running db procedure--------")
            console.table(result)
            return true
        } catch (error) {
            console.error(error)
            return false
        }
     })
})
