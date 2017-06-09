/**
 * Place Parse Cloud Code Here
 */
const gFinance = require('google-finance');
const csv = require('csv');

Parse.Cloud.define('test_push_services', function( req, res) {
    Parse.Push.send({
        data: {
            alert:"This is a test from Parse Server"
        }
    })
    return res.success(null);
});

Parse.Cloud.define('stock_rating', function( req, res ) {
    console.log(req);
    if(!req.params.symbol) { return res.error('Did not provide symbol'); }
    gFinance.historical({
        symbol: req.params.symbol,
        from: '2014-01-01',
        to: '2017-12-31'
    })
    .then(data => {
        return res.success(data);
    })
    .catch(err => {
        return res.error(err);
    });
});

Parse.Cloud.define('update_symbol_table', function( req, res ) {
    console.log(req);
    var query = new Parse.Query()
    return res.success(req);
});

Parse.Cloud.define('all_symbol_files', function( req, res ){
    var SymbolFiles = Parse.Object.extend('SymbolFiles');
    var query = new Parse.Query(SymbolFiles);
    query.useMasterKey = true;
    query.find({
        useMasterKey: true,
        success: function(objects) {
            return res.success(objects);
        },
        error: function(err) {
            console.log(err);
            return res.error(err);
        }
    });
});

/**
 * Parse Cloud Jobs
 */

Parse.Cloud.job('ingest_symbols', function( req, status ) {
    var params = req.params;
    var headers = req.headers;
    var log = req.log;
    status.message('started ingesting symbol files');
    var SymbolFiles = Parse.Object.extend("SymbolFiles");
    var query = new Parse.Query(SymbolFiles);
    query.find({
        useMasterKey: true,
        success: function(objects) {
            console.log(objects);
            console.log(objects.length);
            if(objects.length > 0) {
                console.log("symbol files are available");
                let url = objects[0].get("csv_symbol");
                
                
                Parse.Cloud.httpRequest({url: url.url(),
                    success: function(object) {
                        
                        csv.parse(object.buffer.toString(), {auto_parse: true, relax: true, relax_column_count: true}, (err, result) => {
                            console.log(err);
                            console.log(result);
                            
                            status.success(result);
                        });
                        
                        
                    },
                    error: function(err) {
                        status.error(err);
                    }
                });
            }
            
        },
        error: function(err) {
            console.log(err);
            status.error(err);
        }
    });
});