const pgp = require('pg-promise')();


export async function logToDBStart(db: any, caller: string, method: string): Promise<number> { //Return the ID number of log record
    const cs = new pgp.helpers.ColumnSet(["caller", "method", "timestart", "status"], {table: 'pfdata_logs_collect'})
    let query = await pgp.helpers.insert(
        {
            caller: caller,
            method: method,
            timestart: new Date().toISOString(),
            status: "STARTED", //ERROR, COMPLETE
            
        }
        , cs) + ' RETURNING id';

    let returned;

    try {
        returned = await db.any(query);
        return Promise.resolve(returned[0].id);
    } catch (e) {
        returned = -1;
        console.log(e)
        return Promise.reject(returned)
    }
}

