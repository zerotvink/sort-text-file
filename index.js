const fs = require("fs");
const readline = require("readline");
const { writeFile } = require("fs/promises");
const mergeStream = require("merge-stream");

const MAX_MEMORY_SIZE = 500 * 1024 * 1024;

// Функция для записи строк в файл
async function writeLines(file, lines) {
  return writeFile(file, lines.join("\n") + "\n");
}

// Функция для сортировки и записи временных файлов
async function sortAndWriteTempFile(tempFile, lines) {
  lines.sort();
  await writeLines(tempFile, lines);
}

// Функция для слияния временных файлов в выходной файл
async function mergeFiles(outputFile, tempFiles) {
  const readStreams = tempFiles.map((tempFile) =>
    fs.createReadStream(tempFile)
  );

  const writeStream = fs.createWriteStream(outputFile);
  const mergeStreams = mergeStream(...readStreams);
  mergeStreams.pipe(writeStream);

  return new Promise((resolve) => {
    mergeStreams.on("end", () => {
      tempFiles.forEach((tempFile) => fs.unlinkSync(tempFile));
      resolve();
    });
  });
}

// Функция для внешней сортировки файла
async function externalSort(inputFile, outputFile) {
  const tempFiles = [];
  let lines = [];
  let memorySize = 0;

  const readStream = fs.createReadStream(inputFile);
  const readInterface = readline.createInterface({
    input: readStream,
  });

  for await (const line of readInterface) {
    if (!line?.trim()) {
      continue;
    }

    lines.push(line);
    memorySize += Buffer.byteLength(line);

    if (memorySize >= MAX_MEMORY_SIZE) {
      await sortAndWriteTempFile(`temp-${tempFiles.length}`, lines);
      tempFiles.push(`temp-${tempFiles.length}`);
      lines = [];
      memorySize = 0;
    }
  }

  if (lines.length > 0) {
    await sortAndWriteTempFile(`temp-${tempFiles.length}`, lines);
    tempFiles.push(`temp-${tempFiles.length}`);
  }

  await mergeFiles(outputFile, tempFiles);
}

externalSort("input.txt", "output.txt")
  .then(() => console.log("Сортировка завершена"))
  .catch((err) => console.error(err));
