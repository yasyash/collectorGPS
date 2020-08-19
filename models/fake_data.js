const bookshelf = require( '../api/bookshelf');

module.exports =  bookshelf.Model.extend({

    tableName: 'fake_data'

});