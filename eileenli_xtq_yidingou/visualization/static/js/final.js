// var treeData;

// var oReq = new XMLHttpRequest();
// oReq.onload = reqListener;
// oReq.open("get", "schoolfinal.json", true);
// oReq.send();

// function reqListener(e) {
//     treeData = JSON.parse(this.responseText);
// }

// $(document).ready(function(){
//   $("#b01").click(function(){
//   htmlobj=$.ajax({url:"/js/schoolfinal.json",async:false});
//   $("#myDiv").html(htmlobj.responseText);
//   });
// });


var MongoClient = require('mongodb').MongoClient;
var DB_CONN_STR = 'mongodb://localhost:27017/repo';
 
var insertData = function(db, callback) {  
    //连接到表 site
    var collection = db.collection('eileenli_xtq_yidingou');
    var whereStr = {"name":'schoolfinal'};
  collection.find(whereStr).toArray(function(err, result) {   
    callback(result);
  });
}
 
MongoClient.connect(DB_CONN_STR, function(err, db) {
    console.log("连接成功！");
    selectData(db, function(result) {
    console.log(result);
    db.close();
  });
});