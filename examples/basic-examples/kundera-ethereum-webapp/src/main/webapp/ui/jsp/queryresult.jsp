<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<html>
<head>
    <title> Spring Boot Example</title>
<link href="/bootstrap.min.css" rel="stylesheet">
    <script src="/jquery-2.2.1.min.js"></script>
    <script src="/bootstrap.min.js"></script>
</head>
<body>
<div>
<div class="container" style="margin:50px">
    <div class="row text-center"><strong>Query Result</strong></div>
    <div class="row" style="border:1px solid green;padding:10px">
        <div class="col-md-4 text-center"><strong>blockNumber</strong></div>
        <div class="col-md-4 text-center"><strong>from</strong></div>
        <div class="col-md-4 text-center"><strong>value</strong></div>
    </div>
        <c:forEach var="result" items="${result}">
            <div class="row" style="border:1px solid green;padding:10px">
            <div class="col-md-4 text-center">${result.blockNumber}</div>
            <div class="col-md-4 text-center" >${result.from}</div>
                <div class="col-md-4 text-center">${result.value}</div>
            </div>
        </c:forEach>

</div>
</div>
</body>
</html>