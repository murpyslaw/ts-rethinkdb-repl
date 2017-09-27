import * as dbServer from 'rethinkdb';
import * as color from 'chalk';
import * as _ from 'lodash';

import { URL } from 'url';
import { Connection, ConnectionOptions, Db, CreateResult } from 'rethinkdb';

/**
 * A connection manager to a specific database 'dbName',
 *  on the given rethink-db server URL
 * 
 * @export
 * @class RethinkDBSession
 */
export class RethinkDBSession
{
    /**
     * Wether or not the database 'dbName' was created on 'dbServer' at URL
     * 
     * @static
     * @type {CreateResult}
     * @memberof RethinkDBSession
     */
    public static wasDBCreated = false;
    public static dbInstance: Db;
    /**
     * A connection to the rethink-db server which expires in 'timeout' seconds 
     * @static
     * @type {Connection}
     * @memberof RethinkDBSession
     */
    public static dbServCon: Connection;

    /**
     * 
     * @param {Url} dbUrl A nodejs URL, representing the network path to the rethinkdb instance dbName
     * @param {string} [dbName="default"] The name of the database to create. Defaults to "default".
     * @param {number} [timeout=5] How long to wait before timing out the connection to rethinkdb. Default to 5 seconds.
     * @param {string} [tableName="users"] Public property represeting the default table to add to dbName. Defaults to "users".
     * @param {CreateResult} [wasDBCreated={ created : 0 }] Public property representing the success or failure of creating dbName
     * @memberof RethinkDBSession
     */
    constructor(
        private dbUrl = new URL("rethink://localhost"),
        private timeout: number = 5,
        public tableName: string = "users",
        public dbName: string = "default", 
        public wasDBCreated = false
    ) {
        this.initRethinkSession();
    }

    /**
     * 
     * Firstly, and by default, it waits 5 seconds in an attempt to gracefully connect to 'dbServer', 
     * with an already running rethinkdb instance, at network path 'dbURL'.
     * 
     * Then, it creates a default database 'dbInstance', if one is not present, called 'dbName'.
     * Then, it creates a default table named 'users' in 'dbName', if one does not already exist.
     * 
     * Meanwhile, it sets the public property 'wasDBCreated' to indicate the success of failure of the creation operation.
     * 
     * @memberof RethinkDBSession
     */
    async initRethinkSession () {
        RethinkDBSession.dbServCon = await this.getDBServerConnection();
        RethinkDBSession.dbInstance = await this.getDBInstance();
        await this.createDBIfNotExists();
        await this.createTableIfNotExists("users");
    }
    /**
     * Has 'timeout' seconds to, 
     * asyncronously connect to the rethink database 'dbName', 
     * at network path URL.
     * 
     * @returns {Promise<Connection>} A single awaitable rethink. Connection promise to the database 'dbName'.
     * @memberof RethinkDBSession
     */
    public async getDBServerConnection(): Promise<Connection> {
        let conOpts: ConnectionOptions = {                                                                       // Connect to rethinkDB on DB_HOST:DB_PORT
            db: this.dbName,
            host: this.dbUrl.host,
            port: Number.parseInt(this.dbUrl.port),
            timeout: this.timeout 
        };

        // Await a connection to the rethink-db server
        RethinkDBSession.dbServCon = await dbServer.connect(conOpts);
        // Automatically use 'dbName' for this connection,
        //  becaue that's what the insance of this class represents,
        // a connection to a specific database on the given rethink-db server URL
        RethinkDBSession.dbServCon.use(this.dbName);
        return RethinkDBSession.dbServCon;
    }

    /**
     * Awaits a connection to the rethinkdb server, 
     * and creates a database 'dbName', if it does not exists on the server, while setting 'wasDBCreated' to true if it succeeds.
     * 
     * If in case that it fails, it will print a nice message with the reason why it failed.
     * 
     * @returns {Promise<CreateResult>} A rethink.CreateResult representing wether the database 'dbName' was successfully or not.
     * @memberof RethinkDBSession
     */
    public async createDBIfNotExists() {
        // Retrieves a list of the databases and,
        let doesDBExist = await dbServer.dbList().run(RethinkDBSession.dbServCon);
        // Checks to see if that list contains our 'dbName'.
        if (!_.includes(doesDBExist, this.dbName)) {                                          
            // We try to create 'dbName' on the rethink-db server
            try {
                // If 'dbName' is not on the list, creates a new db with 'dbName'.
                this.wasDBCreated = await dbServer.dbCreate(this.dbName).run(RethinkDBSession.dbServCon)? true : false;
                // If the above action succeeds
                if (this.wasDBCreated)
                {
                    // We print a nice message for the consumer of our class.
                    console.log(color.cyan(
                        "[RethinkDBSession] --- Successfully created new DATABASE --- " + this.dbName
                    )); 
                    // And return to them, 
                    // the promised indication of a successful 'dbName' creation.
                }
                // Unfortunately, here creation of 'dbName' has failed because?
                else
                {
                    this.wasDBCreated = false;
                    console.error(
                        color.magenta("[RethinkDBSession] --- Failed to created new DATABASE --- " + this.dbName)
                    ); 
                }
            }
            // If something goes wrong
            catch (err) {
                // We gracefully tell the consumer of our class that,
                // there was a problem creating 'dbName'
                console.error(
                    color.magenta("[RethinkDBSession] --- I'm sorry, but there was a problem creating" + this.dbName),
                    color.magenta("This is what happened: "),
                    color.red(err)
                );
                return { created: 0 };
            }
        }

        // If the rethink-db server already has 'dbName' in it's list,
        //  we skip creating it and notify the consumer
        return { created: 0 };
    }

    /**
     * Asynchronously return a reference to the database 'dbName'
     * 
     * @private
     * @returns {Promise<Db>} 
     * @memberof RethinkDBSession
     */
    private async getDBInstance(): Promise<Db> {
        if (RethinkDBSession.wasDBCreated) return RethinkDBSession.dbInstance;
        else return dbServer.db(this.dbName);
    }

    private async createTableIfNotExists(tableName: string): Promise<CreateResult> {
        // If there is a DB created
        if (this.wasDBCreated) {

            // TODO: Optimize
            // Retrieve a list of all of the tables in the DB (could be costly?)
            let listResults = await RethinkDBSession.dbInstance.tableList().run(RethinkDBSession.dbServCon);

            // If that table list includes "tableName"
            if (!_.includes(listResults, tableName)) {
                // Try to create "tableName"
                let tableCreated = await RethinkDBSession.dbInstance.tableCreate(tableName).run(RethinkDBSession.dbServCon);
                if (tableCreated) {
                    console.log(color.cyan("DB: " + "created new TABLE --- " + tableName));
                    return tableCreated;
                }
                // Fail return error
                else 
                { 
                    console.log(color.magenta("DB: " + "created new TABLE --- " + tableName));
                    return { created: 0 };
                }
            }
        }

        return { created: 0 };
    }
}