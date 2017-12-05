<html>
   <body>
   <div>
         <form action = "import" method = "POST">
         From Block: <input type = "text" name = "from_block" style="width: 120px;">
         To Block: <input type = "text" name = "to_block" style="width: 120px;">
         <input type = "submit" value = "Import blocks" />
         ${importStatus}
      </form>
      </div>
      
      <div>
      <form action = "queryresult" method = "POST">
         JPA Query: <input type = "text" name = "query" style="width: 500px;">
         <input type = "submit" value = "Run query" />
      </form>
      </div>
      
   </body>
</html>