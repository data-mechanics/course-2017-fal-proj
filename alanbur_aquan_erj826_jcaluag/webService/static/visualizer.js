visualizer={
}
visualizer.documentReady=function(){
	console.log('Visualizer is document ready!')
}
visualizer.getKmeans=function(){
	console.log('Getting kmeans')
	dist=$('#distance').val()
	$.post("/kmeans",{
		distance: dist
	})
	.done(function(response){
		response=JSON.parse(response)
		console.log(response)
	})
}