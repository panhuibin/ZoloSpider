window.addEventListener("DOMContentLoaded",function(){var a=document.querySelector(".js-carousel");if(a){a.classList.remove("is-hidden"),a.offsetHeight;var b=new Flickity(".carousel",{pageDots:!1,setGallerySize:!1,contain:!0,bgLazyLoad:!0,bgLazyLoad:6,percentPosition:!1,cellAlign:"left",cellSelector:".carousel-cell",groupCells:"100%"});a.focus();var c=document.querySelector(".js-remove-onload");setTimeout(function(){c&&c.classList.add("xs-hide")},500),b.on("staticClick",function(c,d,e,f){e.className.match(/no-resize/gi)||(a.classList.toggle("fullscreen"),b.resize(),"number"==typeof f&&b.selectCell(f,!1,!0))});var d,e=document.querySelectorAll(".js-carousel-fullscreen-close");for(d=0;d<e.length;d++)e[d].addEventListener("click",function(){a.classList.remove("fullscreen"),b.resize()});var f,g=document.querySelectorAll(".js-carousel-resize");for(f=0;f<g.length;f++)g[f].addEventListener("click",function(){a.classList.toggle("fullscreen"),b.resize()})}});