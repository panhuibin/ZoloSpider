!function(){"use strict";function a(a){l.push(a),1==l.length&&k()}function b(){for(;l.length;)l[0](),l.shift()}function c(a){this.a=m,this.b=void 0,this.f=[];var b=this;try{a(function(a){f(b,a)},function(a){g(b,a)})}catch(a){g(b,a)}}function d(a){return new c(function(b,c){c(a)})}function e(a){return new c(function(b){b(a)})}function f(a,b){if(a.a==m){if(b==a)throw new TypeError;var c=!1;try{var d=b&&b.then;if(null!=b&&"object"==typeof b&&"function"==typeof d)return void d.call(b,function(b){c||f(a,b),c=!0},function(b){c||g(a,b),c=!0})}catch(b){return void(c||g(a,b))}a.a=0,a.b=b,h(a)}}function g(a,b){if(a.a==m){if(b==a)throw new TypeError;a.a=1,a.b=b,h(a)}}function h(b){a(function(){if(b.a!=m)for(;b.f.length;){var a=b.f.shift(),c=a[0],d=a[1],e=a[2],a=a[3];try{0==b.a?e("function"==typeof c?c.call(void 0,b.b):b.b):1==b.a&&("function"==typeof d?e(d.call(void 0,b.b)):a(b.b))}catch(b){a(b)}}})}function i(a){return new c(function(b,c){function d(c){return function(d){g[c]=d,f+=1,f==a.length&&b(g)}}var f=0,g=[];0==a.length&&b(g);for(var h=0;h<a.length;h+=1)e(a[h]).c(d(h),c)})}function j(a){return new c(function(b,c){for(var d=0;d<a.length;d+=1)e(a[d]).c(b,c)})}var k,l=[];k=function(){setTimeout(b)};var m=2;c.prototype.g=function(a){return this.c(void 0,a)},c.prototype.c=function(a,b){var d=this;return new c(function(c,e){d.f.push([a,b,c,e]),h(d)})},window.Promise||(window.Promise=c,window.Promise.resolve=e,window.Promise.reject=d,window.Promise.race=j,window.Promise.all=i,window.Promise.prototype.then=c.prototype.c,window.Promise.prototype.catch=c.prototype.g)}(),function(){"use strict";function a(a,b){j?a.addEventListener("scroll",b,!1):a.attachEvent("scroll",b)}function b(a){document.body?a():j?document.addEventListener("DOMContentLoaded",a):document.onreadystatechange=function(){"interactive"==document.readyState&&a()}}function c(a){this.a=document.createElement("div"),this.a.setAttribute("aria-hidden","true"),this.a.appendChild(document.createTextNode(a)),this.b=document.createElement("span"),this.c=document.createElement("span"),this.h=document.createElement("span"),this.f=document.createElement("span"),this.g=-1,this.b.style.cssText="display:inline-block;position:absolute;height:100%;width:100%;overflow:scroll;font-size:16px;",this.c.style.cssText="display:inline-block;position:absolute;height:100%;width:100%;overflow:scroll;font-size:16px;",this.f.style.cssText="display:inline-block;position:absolute;height:100%;width:100%;overflow:scroll;font-size:16px;",this.h.style.cssText="display:inline-block;width:200%;height:200%;font-size:16px;",this.b.appendChild(this.h),this.c.appendChild(this.f),this.a.appendChild(this.b),this.a.appendChild(this.c)}function d(a,b){a.a.style.cssText="min-width:20px;min-height:20px;display:inline-block;overflow:hidden;position:absolute;width:auto;margin:0;padding:0;top:-999px;left:-999px;white-space:nowrap;font:"+b+";"}function e(a){var b=a.a.offsetWidth,c=b+100;return a.f.style.width=c+"px",a.c.scrollLeft=c,a.b.scrollLeft=a.b.scrollWidth+100,a.g!==b&&(a.g=b,!0)}function f(b,c){function d(){var a=f;e(a)&&null!==a.a.parentNode&&c(a.g)}var f=b;a(b.b,d),a(b.c,d),e(b)}function g(a,b){var c=b||{};this.family=a,this.style=c.style||"normal",this.weight=c.weight||"normal",this.stretch=c.stretch||"normal"}function h(){if(null===l){var a=document.createElement("div");try{a.style.font="condensed 100px sans-serif"}catch(a){}l=""!==a.style.font}return l}function i(a,b){return[a.style,a.weight,h()?a.stretch:"","100px",b].join(" ")}var j=!!document.addEventListener,k=null,l=null,m=!!window.FontFace;g.prototype.a=function(a,e){var g=this,h=a||"BESbswy",j=e||3e3,l=(new Date).getTime();return new Promise(function(a,e){if(m){var n=function(){(new Date).getTime()-l>=j?e(g):document.fonts.load(i(g,g.family),h).then(function(b){1<=b.length?a(g):setTimeout(n,25)},function(){e(g)})};n()}else b(function(){function b(){var b;(b=-1!=q&&-1!=r||-1!=q&&-1!=s||-1!=r&&-1!=s)&&((b=q!=r&&q!=s&&r!=s)||(null===k&&(b=/AppleWebKit\/([0-9]+)(?:\.([0-9]+))/.exec(window.navigator.userAgent),k=!!b&&(536>parseInt(b[1],10)||536===parseInt(b[1],10)&&11>=parseInt(b[2],10))),b=k&&(q==t&&r==t&&s==t||q==u&&r==u&&s==u||q==v&&r==v&&s==v)),b=!b),b&&(null!==w.parentNode&&w.parentNode.removeChild(w),clearTimeout(x),a(g))}function m(){if((new Date).getTime()-l>=j)null!==w.parentNode&&w.parentNode.removeChild(w),e(g);else{var a=document.hidden;!0!==a&&void 0!==a||(q=n.a.offsetWidth,r=o.a.offsetWidth,s=p.a.offsetWidth,b()),x=setTimeout(m,50)}}var n=new c(h),o=new c(h),p=new c(h),q=-1,r=-1,s=-1,t=-1,u=-1,v=-1,w=document.createElement("div"),x=0;w.dir="ltr",d(n,i(g,"sans-serif")),d(o,i(g,"serif")),d(p,i(g,"monospace")),w.appendChild(n.a),w.appendChild(o.a),w.appendChild(p.a),document.body.appendChild(w),t=n.a.offsetWidth,u=o.a.offsetWidth,v=p.a.offsetWidth,m(),f(n,function(a){q=a,b()}),d(n,i(g,'"'+g.family+'",sans-serif')),f(o,function(a){r=a,b()}),d(o,i(g,'"'+g.family+'",serif')),f(p,function(a){s=a,b()}),d(p,i(g,'"'+g.family+'",monospace'))})})},window.FontFaceObserver=g,window.FontFaceObserver.prototype.check=g.prototype.a,"undefined"!=typeof module&&(module.exports=window.FontFaceObserver)}();