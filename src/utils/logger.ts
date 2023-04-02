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

export async function logToDBEnd(db: any, id: number, status: string, errors: number, logpath: string): Promise<boolean> { //Return the ID number of log record
    const cs = new pgp.helpers.ColumnSet(["timeend", "status", "errorcount", "logpath"], {table: 'pfdata_logs_collect'})
    let query = await pgp.helpers.update(
        {
            timeend: new Date().toISOString(),
            status: status,
            errorcount: errors, //ERROR, COMPLETE
            logpath: logpath
        }
        , cs) + pgp.as.format('WHERE id = ${id}', {id});;

    await db.any(query)

    return true;
}

