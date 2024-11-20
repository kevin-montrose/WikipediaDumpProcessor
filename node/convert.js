const Parser = require('wikiparser-node');
const FS = require("fs");

// here we rewrite the templates we care about into cites 
Parser.templates.set(
	'Template:Sfn',
	'<cite>{{{loc}}}</cite>',
);
Parser.templates.set(
	'Template:Sfnm',
	'<cite>{{{loc}}}</cite>',
);
Parser.templates.set(
	'Template:Cite_web',
	'<cite>{{{url}}}</cite>',
);

const buffer = new Int8Array(4 * 1024);
let pending = new Int8Array(0);
let read;
while ((read = FS.readSync(0, buffer, 0, buffer.length)) != 0) {
	const readBytes = buffer.slice(0, read);
	const endByte = readBytes.indexOf(7); // bell character

	const newPending = new Int8Array(pending.length + readBytes.length);
	newPending.set(pending, 0);
	newPending.set(readBytes, pending.length);

	const lastPendingLength = pending.length;

	pending = newPending;

	if (endByte != -1) {
		const toDecode = new Int8Array(lastPendingLength + endByte);
		toDecode.set(newPending.slice(0, toDecode.length), 0);

		const decoder = new TextDecoder("utf-8");
		const markupFile = decoder.decode(toDecode);

		const markup = FS.readFileSync(markupFile, 'utf-8');

		const parsed = Parser.parse(markup);
		const asHtml = parsed.toHtml();

		process.stdout.write(asHtml);
		process.stdout.write("\x07");

		const trailing = new Int8Array(pending.length - toDecode.length - 1);
		trailing.set(newPending.slice(toDecode.length + 1), 0);

		pending = trailing;
	}
}