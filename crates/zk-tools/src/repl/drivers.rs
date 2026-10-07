//! Fixed in-memory REPL drivers. User code travels only over stdin after ownership
//! is committed. No interactive shell history, init script, or generated source file.
pub(super) fn command(language: &str) -> Option<(&'static str, Vec<String>)> {
    let (program, args): (&str, Vec<&str>) = match language {
        "python" => ("python3", vec!["-I", "-B", "-u", "-c", PYTHON]),
        "node" => ("node", vec!["-e", NODE]),
        "ruby" => ("ruby", vec!["-e", RUBY]),
        _ => return None,
    };
    Some((program, args.into_iter().map(str::to_owned).collect()))
}
const PYTHON: &str = r"
import sys,json,ast,io,traceback,contextlib,os
sys.path.insert(0,os.getcwd())
class Capture(io.TextIOBase):
 def __init__(self): self.parts=[];self.size=0;self.truncated=False
 def write(self,value):
  text=str(value);raw=text.encode('utf-8',errors='replace');limit=max(0,102400-self.size)
  if limit: self.parts.append(raw[:limit].decode('utf-8',errors='ignore'))
  self.size+=min(len(raw),limit);self.truncated|=len(raw)>limit
  return len(text)
 def flush(self): pass
 def value(self): return ''.join(self.parts)
scope={'__name__':'__main__'}
wire=sys.stdout
for line in sys.stdin:
 try: request=json.loads(line)
 except Exception: break
 out,err=Capture(),Capture();failed=False
 with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
  try:
   tree=ast.parse(request['code'],'<repl>','exec')
   if tree.body and isinstance(tree.body[-1],ast.Expr):
    last=tree.body.pop();exec(compile(tree,'<repl>','exec'),scope)
    value=eval(compile(ast.Expression(last.value),'<repl>','eval'),scope)
    if value is not None: print(repr(value))
   else: exec(compile(tree,'<repl>','exec'),scope)
  except BaseException:
   failed=True;traceback.print_exc()
 wire.write(json.dumps({'id':request['id'],'stdout':out.value(),'stderr':err.value(),'isError':failed,'truncated':out.truncated or err.truncated},ensure_ascii=True)+'\n');wire.flush()
";
const NODE: &str = r"
const readline=require('readline'),repl=require('repl'),stream=require('stream'),util=require('util');
const wire=process.stdout.write.bind(process.stdout);
let output=null,errors=null;
process.stdout.write=(chunk,encoding,callback)=>{if(output)capture(output,Buffer.isBuffer(chunk)?chunk.toString():chunk);const done=typeof encoding==='function'?encoding:callback;if(typeof done==='function')done();return true;};
process.stderr.write=(chunk,encoding,callback)=>{if(errors)capture(errors,Buffer.isBuffer(chunk)?chunk.toString():chunk);const done=typeof encoding==='function'?encoding:callback;if(typeof done==='function')done();return true;};
function capture(target,value){let text=String(value),raw=Buffer.from(text),remain=Math.max(0,102400-target.size);if(remain)target.parts.push(raw.subarray(0,remain).toString());target.size+=Math.min(raw.length,remain);target.truncated ||= raw.length>remain;}
const evaluator=repl.start({input:new stream.PassThrough(),output:new stream.Writable({write(_chunk,_enc,done){done()}}),terminal:false,prompt:'',useGlobal:false});
function evaluate(code){return new Promise((resolve,reject)=>{if(!evaluator._domain){reject(new Error('NODE_REPL_DOMAIN_UNAVAILABLE'));return;}let settled=false;const finish=(error,value)=>{if(settled)return;settled=true;evaluator._domain.removeListener('error',onError);error?reject(error):resolve(value);};const onError=error=>finish(error);evaluator._domain.on('error',onError);evaluator.eval(code+'\n',evaluator.context,'<repl>',finish);});}
evaluator.context.console={log:(...a)=>capture(output,util.format(...a)+'\n'),info:(...a)=>capture(output,util.format(...a)+'\n'),warn:(...a)=>capture(errors,util.format(...a)+'\n'),error:(...a)=>capture(errors,util.format(...a)+'\n')};
(async()=>{for await(const line of readline.createInterface({input:process.stdin,crlfDelay:Infinity})){
 let request;try{request=JSON.parse(line)}catch{break}
 output={parts:[],size:0,truncated:false};errors={parts:[],size:0,truncated:false};let failed=false;
 try{let value=await evaluate(request.code);if(value&&typeof value.then==='function')value=await value;if(value!==undefined)capture(output,util.inspect(value,{depth:5,maxArrayLength:100,maxStringLength:10000})+'\n');}catch(error){failed=true;capture(errors,String(error.stack||error)+'\n');}
 wire(JSON.stringify({id:request.id,stdout:output.parts.join(''),stderr:errors.parts.join(''),isError:failed,truncated:output.truncated||errors.truncated})+'\n');
}evaluator.close();})();
";
const RUBY: &str = r##"
require 'json'
class Capture
 attr_reader :truncated
 def initialize; @parts=[];@size=0;@truncated=false;end
 def write(value); text=value.to_s;remain=[0,102400-@size].max;part=text.byteslice(0,remain).to_s.force_encoding('UTF-8').scrub('');@parts << part unless part.empty?;@size+=part.bytesize;@truncated ||= text.bytesize>remain;text.bytesize;end
 def puts(*values);values.each{|v|write(v);write("\n")};end
 def print(*values);values.each{|v|write(v)};end
 def flush;end
 def value;@parts.join;end
end
scope=TOPLEVEL_BINDING;wire=STDOUT;wire.sync=true
STDIN.each_line do |line|
 begin;request=JSON.parse(line);rescue;break;end
 out=Capture.new;err=Capture.new;failed=false;previous_out=$stdout;previous_err=$stderr
 begin
  $stdout=out;$stderr=err
  result=eval(request.fetch('code'),scope,'<repl>');out.puts(result.inspect) unless result.nil?
 rescue Exception => error
  failed=true;err.puts("#{error.class}: #{error.message}");err.puts(error.backtrace.first(20).join("\n"))
 ensure
  $stdout=previous_out;$stderr=previous_err
 end
 wire.write(JSON.generate({'id'=>request['id'],'stdout'=>out.value,'stderr'=>err.value,'isError'=>failed,'truncated'=>out.truncated||err.truncated})+"\n")
end
"##;
