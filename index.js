var AWS = require('aws-sdk'),
    _ = require('underscore');

module.exports = (function() {

    var getMatchIndices = require('waterline-criteria');

    //Tell me what environment variables exist
    console.log("AWS environment variables available:")
    _.each(process.env, function (item, key) {
        if (key.indexOf('AWS') > -1) {
            console.log("\t", key, '\t', item);
        }
    })

    //For some reason, AWS is not detecting my env variables.
    AWS.config.update({accessKeyId:process.env.AWS_ACCESS_KEY, secretAccessKey:process.env.AWS_SECRET_ACCESS_KEY});
    AWS.config.update({region:process.env.AWS_REGION});

    function createPrefix (config, collectionName) {
        return (config.prefix + collectionName).toLowerCase()
    }

    function createKey (config, collectionName, id) {
        return createPrefix(config, collectionName) + "/" + id.toLowerCase();
    }

    // You'll want to maintain a reference to each collection
    // (aka model) that gets registered with this adapter.
    var _modelReferences = {};

    // You may also want to store additional, private data
    // per-collection (esp. if your data store uses persistent
    // connections).
    //
    // Keep in mind that models can be configured to use different databases
    // within the same app, at the same time.
    //
    // i.e. if you're writing a MariaDB adapter, you should be aware that one
    // model might be configured as `host="localhost"` and another might be using
    // `host="foo.com"` at the same time.  Same thing goes for user, database,
    // password, or any other config.
    //
    // You don't have to support this feature right off the bat in your
    // adapter, but it ought to get done eventually.
    //
    // Sounds annoying to deal with...
    // ...but it's not bad.  In each method, acquire a connection using the config
    // for the current model (looking it up from `_modelReferences`), establish
    // a connection, then tear it down before calling your method's callback.
    // Finally, as an optimization, you might use a db pool for each distinct
    // connection configuration, partioning pools for each separate configuration
    // for your adapter (i.e. worst case scenario is a pool for each model, best case
    // scenario is one single single pool.)  For many databases, any change to
    // host OR database OR user OR password = separate pool.
    var _dbPools = {};

    var adapter = {
        syncable: true, // to track schema internally

        defaults: {
            prefix: '',
            //private|public-read|public-read-write|authenticated-read|
            // bucket-owner-read|bucket-owner-full-control
            acl: "bucket-owner-full-control",
            serverSideEncryption: "AES256",
            //STANDARD|REDUCED_REDUNDANCY
            storageClass: "REDUCED_REDUNDANCY",
            schema: false,
            nativeParser: false,
            safe: true
        },

        /**
         *
         * This method runs when a model is initially registered
         * at server-start-time.  This is the only required method.
         *
         * @param  {{identity:*}}   collection [description]
         * @param  {Function} cb         [description]
         */
        registerCollection: function(collection, cb) {

            // Keep a reference to this collection
            _modelReferences[collection.identity] = collection;
            return cb();
        },


        /**
         * Fired when a model is unregistered, typically when the server
         * is killed. Useful for tearing-down remaining open connections,
         * etc.
         *
         * @param  {Function} cb [description]
         * @return {[type]}      [description]
         */
        teardown: function(cb) {
            cb();
        },



        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   definition     [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        define: function(collectionName, definition, cb) {

            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // Define a new "table" or "collection" schema in the data store
            cb();
        },

        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        describe: function(collectionName, cb) {

            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // Respond with the schema (attributes) for a collection or table in the data store
            var attributes = {};
            cb(null, attributes);
        },


        /**
         *
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   relations      [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        drop: function(collectionName, relations, cb) {
            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // Drop a "table" or "collection" schema from the data store
            cb();
        },




        // OVERRIDES NOT CURRENTLY FULLY SUPPORTED FOR:
        //
        // alter: function (collectionName, changes, cb) {},
        // addAttribute: function(collectionName, attrName, attrDef, cb) {},
        // removeAttribute: function(collectionName, attrName, attrDef, cb) {},
        // alterAttribute: function(collectionName, attrName, attrDef, cb) {},
        // addIndex: function(indexName, options, cb) {},
        // removeIndex: function(indexName, options, cb) {},



        /**
         *
         * REQUIRED method if users expect to call Model.find(), Model.findOne(),
         * or related.
         *
         * You should implement this method to respond with an array of instances.
         * Waterline core will take care of supporting all the other different
         * find methods/usages.
         *
         * @param  {string} collectionName
         * @param  {{}} options
         * @param  {Function} cb callback
         */
        find: function(collectionName, options, cb) {
            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName],
                config = this.config,
                params = {
                    Bucket: this.config.bucketName,
                    Prefix: createPrefix(config, collectionName)
                };

            // Options object is normalized for you:
            //
            // options.where
            // options.limit
            // options.skip
            // options.sort

            if (options.where && options.where.id) {
                params.Prefix += "/" + options.where.id
            }

            if (options.limit) {
                params.MaxKeys = options.limit;
            }

            // Filter, paginate, and sort records from the datastore.
            // You should end up w/ an array of objects as a result.
            // If no matches were found, this will be an empty array.

            var s3 = new AWS.S3();
            s3.listObjects(params, function (err, data) {
                // Respond with error or an array of updated records.
                console.log("find", err, data);
                if (data && data.Contents) {
                    _.each(data.Contents, function (item) {
                        item.id = item.Key.replace(createPrefix(config, collectionName) + "/", '');
                    });
                }

                console.log("find", err, data);
                cb(err, data && data.Contents);
            });
//            s3.getObject({
//                Bucket: "",
//                //ETag has to match this or 412
//                IfMatch: "",
//                //Must be modified or 304
//                IfModifiedSince: new Date(),
//                //Only if ETag is different or 304
//                IfNoneMatch: "",
//                Key: "",
//                ResponseCacheControl: "",
//                ResponseContentDisposition: "",
//                ResponseContentEncoding: "",
//                ResponseContentLanguage: "",
//                ResponseContentType: "",
//                ResponseExpires: "",
//                VersionId: ""
//            }, function (err, data) {
//                // Respond with error or an array of updated records.
//                console.log(err, data);
//                cb(err, data);
//            });
        },

        /**
         *
         * REQUIRED method if users expect to call Model.create() or any methods
         *
         * @param  {string}   collectionName
         * @param  {{}}   values
         * @param  {Function} cb callback
         */
        create: function(collectionName, values, cb) {
            //assertion
            values.id = Math.floor((Math.random() * 99999999999)).toString(36);

            // If you need to access your private data for this collection:
            var buffer = new Buffer(values.body,'base64'),
                collection = _modelReferences[collectionName],
                params = {
                    Bucket: this.config.bucketName,
                    Key: createKey(this.config, collectionName, values.id),
                    Body: buffer,
                    ServerSideEncryption: this.config.serverSideEncryption,
                    StorageClass: values.storageClass || this.config.storageClass
                };

            //if they want to use the server settings, make it empty
            // otherwise, default to secure
            if (this.config.acl) {
                params.ACL = this.config.acl;
            }

            if (values.metadata) {
                params.Metadata = values.metadata;
            }

            if (values.expires) {
                params.Expires = values.expires;
            }

            if (values.contentMD5) {
                params.ContentMD5 = values.contentMD5;
            }

            if (values.contentLength) {
                params.ContentLength = values.contentLength;
            }

            // Create a single new model (specified by `values`)
            var s3 = new AWS.S3();
            s3.putObject(params, function (err, data) {
                if (data) {
                    data.id = values.id;
                }

                console.log("create", err, data);
                cb(err, data);
            });
        },

        /**
         *
         *
         * REQUIRED method if users expect to call Model.update()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        update: function(collectionName, options, values, cb) {

            // If you need to access your private data for this collection:
            var collection = _modelReferences[collectionName];

            // 1. Filter, paginate, and sort records from the datastore.
            //    You should end up w/ an array of objects as a result.
            //    If no matches were found, this will be an empty array.
            //
            // 2. Update all result records with `values`.
            //
            // (do both in a single query if you can-- it's faster)

            throw new Error("Not supported yet.");
            /*
            var s3 = new AWS.S3();
            s3.getObject({bucket: "", key: ""}, function (err, data) {
                // Respond with error or an array of updated records.
                console.log(err, data);
                cb(err, data);
            });
            */
        },

        /**
         *
         * REQUIRED method if users expect to call Model.destroy()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        destroy: function(collectionName, options, cb) {
            //might be a good idea to fetch all matches first, then they can be returned after
            // the items are deleted for good behaviour.

            if (options && options.where && options.where.id) {
                // If you need to access your private data for this collection:
                var collection = _modelReferences[collectionName],
                    params = {
                        Bucket: this.config.bucketName,
                        Delete: {
                            Objects: [
                                {Key: createKey(this.config, collectionName, options.where.id)}
                            ]
                        }
                    };

                // 1. Filter, paginate, and sort rcreateKey(this.config, collectionName, options.where.id)ecords from the datastore.
                //    You should end up w/ an array of objects as a result.
                //    If no matches were found, this will be an empty array.
                //
                // 2. Destroy all result records.
                //
                // (do both in a single query if you can-- it's faster)

                var s3 = new AWS.S3();
                s3.deleteObjects(params, function (err, data) {
                    console.log(err, data);
                    cb(err, data);
                });
            } else {
                cb(new Error("Missing id"), null);
            }
        },



        /*
         **********************************************
         * Optional overrides
         **********************************************

         // Optional override of built-in batch create logic for increased efficiency
         // (since most databases include optimizations for pooled queries, at least intra-connection)
         // otherwise, Waterline core uses create()
         createEach: function (collectionName, arrayOfObjects, cb) { cb(); },

         // Optional override of built-in findOrCreate logic for increased efficiency
         // (since most databases include optimizations for pooled queries, at least intra-connection)
         // otherwise, uses find() and create()
         findOrCreate: function (collectionName, arrayOfAttributeNamesWeCareAbout, newAttributesObj, cb) { cb(); },
         */


        /*
         **********************************************
         * Custom methods
         **********************************************

         ////////////////////////////////////////////////////////////////////////////////////////////////////
         //
         // > NOTE:  There are a few gotchas here you should be aware of.
         //
         //    + The collectionName argument is always prepended as the first argument.
         //      This is so you can know which model is requesting the adapter.
         //
         //    + All adapter functions are asynchronous, even the completely custom ones,
         //      and they must always include a callback as the final argument.
         //      The first argument of callbacks is always an error object.
         //      For core CRUD methods, Waterline will add support for .done()/promise usage.
         //
         //    + The function signature for all CUSTOM adapter methods below must be:
         //      `function (collectionName, options, cb) { ... }`
         //
         ////////////////////////////////////////////////////////////////////////////////////////////////////


         // Custom methods defined here will be available on all models
         // which are hooked up to this adapter:
         //
         // e.g.:
         //
         foo: function (collectionName, options, cb) {
         return cb(null,"ok");
         },
         bar: function (collectionName, options, cb) {
         if (!options.jello) return cb("Failure!");
         else return cb();
         }

         // So if you have three models:
         // Tiger, Sparrow, and User
         // 2 of which (Tiger and Sparrow) implement this custom adapter,
         // then you'll be able to access:
         //
         // Tiger.foo(...)
         // Tiger.bar(...)
         // Sparrow.foo(...)
         // Sparrow.bar(...)


         // Example success usage:
         //
         // (notice how the first argument goes away:)
         Tiger.foo({}, function (err, result) {
         if (err) return console.error(err);
         else console.log(result);

         // outputs: ok
         });

         // Example error usage:
         //
         // (notice how the first argument goes away:)
         Sparrow.bar({test: 'yes'}, function (err, result){
         if (err) console.error(err);
         else console.log(result);

         // outputs: Failure!
         })




         */


    };


    // Expose adapter definition
    return adapter;

})();