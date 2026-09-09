const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const html=fs.readFileSync('src/templates/dashboard.html','utf8');
const script=html.match(/<script>\s*([\s\S]*?)<\/script>/)[1];

function ui(){
  let markers=[],prices=[];
  const elements=new Map();
  function el(id){if(!elements.has(id))elements.set(id,{value:'',textContent:'',style:{},addEventListener(){},replaceChildren(){},append(){}});return elements.get(id)}
  const context={document:{getElementById:el,querySelectorAll:()=>[]},window:{addEventListener(){}},
    LightweightCharts:{CrosshairMode:{Normal:0},CandlestickSeries:0,
      createChart:()=>({addSeries:()=>({setData:d=>prices=d}),timeScale:()=>({fitContent(){}}),remove(){}}),
      createSeriesMarkers:(_,m)=>{markers=m}},console};
  vm.createContext(context);
  vm.runInContext(script.replace('loadEvents().then(loadResult).catch(showStartupError);',''),context);
  return {context,el,get markers(){return markers},get prices(){return prices}};
}
for(const present of [true,false])test('release marker does not move to a nearby bar: '+present,()=>{
  const u=ui();const released='2026-08-12T12:30:00Z';
  const time=present?released:'2026-08-12T12:29:00Z';
  u.context.bars=[{window_start:time,open:100,high:101,low:99,close:100}];
  vm.runInContext(`drawChart(bars,'${released}')`,u.context);
  assert.equal(u.prices.length,1);
  assert.equal(u.markers.length,present?1:0);
  if(present)assert.equal(u.markers[0].time,Date.parse(released)/1000);
  assert.match(u.el('coverage-note').textContent,/2026-08-12/);
});
test('empty chart retains exact official timestamp',()=>{
  const u=ui();vm.runInContext("drawChart([],'2026-08-12T12:30:00Z')",u.context);
  assert.match(u.el('coverage-note').textContent,/2026-08-12/);
});
test('repeated analytics loads reuse the two chart instances',()=>{
  const u=ui();let initialized=0,updated=0;
  u.context.echarts={init:()=>{initialized++;return {setOption(){updated++},resize(){}}}};
  for(let i=0;i<5;i++)vm.runInContext('drawAnalytics({points:[]},{points:[]})',u.context);
  assert.equal(initialized,2);
  assert.equal(updated,10);
});
test('timeframe reload only fetches the dependent bars',async()=>{
  const u=ui();const requests=[];
  u.context.fetch=async url=>{requests.push(url);return {ok:true,json:async()=>[]}};
  vm.runInContext("state.activeBase='/api/v1/events/example/symbols/NVDA';state.releasedAt='2026-08-12T12:30:00Z'",u.context);
  for(const timeframe of ['1m','3m','5m','1m']){
    vm.runInContext(`state.timeframe='${timeframe}'`,u.context);
    await vm.runInContext('loadBars()',u.context);
  }
  assert.deepEqual(requests,['1m','3m','5m','1m'].map(t=>'/api/v1/events/example/symbols/NVDA/bars?timeframe='+t));
});
test('release selector date stays on the UTC release day',()=>{
  const u=ui();
  assert.equal(vm.runInContext("releaseDateLabel('2026-07-29T18:00:00Z')",u.context),'2026-07-29 UTC');
});
