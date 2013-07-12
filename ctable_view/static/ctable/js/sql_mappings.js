function SqlExtractMapping(data) {
    var self = this;

    var config = {
        'columns': {
            create: function(options) {
                return new ColumnDef(options.data);
            }
        }
    }
    self._id = ko.observable();

    ko.mapping.fromJS(data.sql_mapping, config, self);

    self.addColumn = function() {
        var d = new ColumnDef({
            name: '', data_type: '', null_value_placeholder: null,
            date_format: null, max_length: null,
            value_source: '', value_attribute: null, value_index: null,
            match_keys: []
        });
        self.columns.push(d);
        d.startEditing();
    }

    self.removeColumn = function(column) {
        self.columns.remove(column);
    }

    self._id.show_day = ko.computed(function() {
        return self.schedule_type() != 'daily';
    }, self);

    self.validate = function() {
       self.validate.error('');
        if (!self.name()){
            self.validate.error('Must have a name.');
            return false;
        } else if (!/^[A-Za-z\d_]+$/.test(self.name())) {
            self.validate.error('Illegal characters in name.');
            return false;
        }
        if (self.columns().length == 0) {
            self.validate.error('Must have at least one column.');
            return false;
        }
        return true;
    }
    self.validate.error = ko.observable('');

    self.save = function() {
        if (self.validate()) {
            var data = ko.mapping.toJSON(self);
            $.post(self._id() || '', data, function (response) {
                if (response.error) {
                    self.showMessage(response.error, 'error');
                } else {
                    window.location.href = response.redirect;
                }
            }, 'json').fail(function() { self.showMessage('Saving failed', 'error'); })
        }
    }

    self.showMessage = function(text, style) {
        $('#message p').text(text);
        $('#message').attr('class', 'alert alert-'+style);
        $('#message').show();
        $('html,body').scrollTop(0);
    }
}

function ColumnDef(json) {
   var self = this;
   ko.mapping.fromJS(json, {
            match_keys: {
                include: ['index', 'value']
            }}, self);

   self.addMatcher = function() {
       self.match_keys.push(ko.mapping.fromJS({
           index: 1, value: ''
       }, {}));
   }

   self.removeMatcher = function(matcher) {
       self.match_keys.remove(matcher);
   }

   self.data_type.show_date_format = ko.computed(function() {
       return self.data_type() == 'date';
   }, self);

   self.data_type.show_max_length = ko.computed(function() {
       return self.data_type() == 'string';
   }, self);

   self.value_source.value_key = ko.computed(function() {
       if (self.value_attribute()) {
           return "['" + self.value_attribute() + "']";
       } else if (self.value_index() != null && self.value_index() != NaN) {
           return "[" + self.value_index() + "]";
       } else {
           return "";
       }
   }, self);

   self.validate = function() {
       self.validate.error('');
       if (!self.name()){
           self.validate.error('Column must have a name.');
           return false;
       } else if (!/^[A-Za-z\d_]+$/.test(self.name())) {
           self.validate.error('Illegal characters in name.');
           return false;
       }
       if (self.value_source() === 'key') {
           self.value_attribute(null)
           var index = self.value_index();
           if (index == null || index == NaN || index < 0) {
               self.validate.error('Must supply index > 0 for key value source.');
               return false;
           }
       } else if (self.value_source() === 'value') {
           if ($.trim(self.value_attribute()) === '') {
               self.value_attribute(null);
           }
       }
       return true;
   }
   self.validate.error = ko.observable('');

   self.name.editing = ko.observable(false);
   self.startEditing = function () {
       self.name.editing(true);
   };
   self.stopEdit = function () {
       self.name.editing(!self.validate());
   };
}

ko.bindingHandlers.numericValue = {
    init : function(element, valueAccessor, allBindingsAccessor) {
        var underlyingObservable = valueAccessor();
        var interceptor = ko.dependentObservable({
            read: underlyingObservable,
            write: function(value) {
                if ($.isNumeric(value)) {
                    underlyingObservable(parseFloat(value));
                } else if (value == '') {
                    underlyingObservable(null);
                }
            }
        });
        ko.bindingHandlers.value.init(element, function() { return interceptor }, allBindingsAccessor);
    },
    update : ko.bindingHandlers.value.update
};

ko.bindingHandlers.csvValue = {
    init : function(element, valueAccessor, allBindingsAccessor) {
        var underlyingObservable = valueAccessor();
        var interceptor = ko.dependentObservable({
            read: underlyingObservable,
            write: function(value) {
                underlyingObservable(value.split(/\s*,\s*/));
            }
        });
        ko.bindingHandlers.value.init(element, function() { return interceptor }, allBindingsAccessor);
    },
    update : ko.bindingHandlers.value.update
};