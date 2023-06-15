// import path from "path";
// import fs from "fs";
// import { Worker, isMainThread, workerData, parentPort } from "worker_threads"; 
// import csv from "csv-parser";


// class Converter {
//     constructor(directoryPath) {
//         this.directoryPath = directoryPath;
//     }

//     start() {
//         return new Promise((resolve, reject) => {
//             if(isMainThread) {
//                 fs.readdir(this.directoryPath, (err, files) => {
//                     if (err) {
//                         console.error(err);
//                         reject(err);
//                         return;
//                     }
    
//                     const csvFiles = files.filter((file) => path.extname(file).toLowerCase() === ".csv");
    
//                     if (csvFiles.length === 0) {
//                         console.error("No CSV files!");
//                         reject(new Error("No CSV files!"));
//                         return;
//                     }
    
//                     const filesCount = csvFiles.length;
//                     const startTime = new Date();
    
//                     const workersCount = filesCount < 10 ? filesCount : 10;
//                     const recordsPerWorker = Math.ceil(filesCount / workersCount);
    
//                     let completedWorkers = 0;
//                     let overallDuration = 0;
//                     let ovearallRecordCount = 0;

//                     for (let i = 0; i < workersCount; i++) {
//                         const start = i * recordsPerWorker;
//                         const end = start + recordsPerWorker;

//                         const worker = new Worker("./converter.js", {
//                             workerData: {
//                                 directoryPath: this.directoryPath,
//                                 csvFiles: csvFiles.slice(start, end)
//                             }
//                         })

//                         worker.on("message", (message) => {

//                             const endTime = new Date();
//                             const duration = endTime - startTime;
//                             overallDuration += duration;
//                             ovearallRecordCount += message.recordCount;
                             
                  
//                             completedWorkers ++;
                  
//                             if (completedWorkers === workersCount) {
//                                 console.log(`Total record count: ${ovearallRecordCount}. Duration: ${overallDuration}ms`); 
//                                 resolve();
//                             }
//                           });
                    
//                           worker.on("error", (err) => {
//                             console.error(err);
//                             reject(err);
//                           });

//                           worker.on('exit', (code) => {
//                             if (code !== 0) {
//                               const errorMessage = `Worker stopped with exit code ${code}`;
//                               console.error(errorMessage);
//                               reject(new Error(errorMessage));
//                             }
//                           });
//                     }
    
//                 })
//             } else {
//                 const { csvFiles, directoryPath } = workerData;

//                 csvFiles.forEach((csvFile) => {

//                     const csvFilePath = path.join(directoryPath, csvFile);
//                     const jsonFilePath = path.join("converted", `${path.parse(csvFile).name}.json`);
                
//                     let recordCount = 0;
//                     const records = [];
                
//                     fs.createReadStream(csvFilePath)
//                       .pipe(csv())
//                       .on("data", (data) => {
//                         records.push(data);
//                         recordCount++;
//                       })
//                       .on("end", () => {
//                         fs.writeFile(jsonFilePath, JSON.stringify(records, null, 2), (err) => {
//                           if (err) {
//                             console.error("Error writing JSON file:", err);
//                             reject(err);
//                             return;
//                           }
//                         });
                
//                         parentPort.postMessage(recordCount);
//                       });
//                   }); 
//             }
            
//         })
//     }
// }

// const directoryPath = process.argv[2];
//  console.log(directoryPath)

// if (!directoryPath) {
//   console.error('No such directory!');
//   process.exit();
// }

// const converter = new Converter(directoryPath);
// converter.start()
//   .then(() => {
//     console.log('Success!');
//     process.exit();
//   })
//   .catch(err => {
//     console.error('Failed!', err);
//     process.exit();
//   });


import path from "path";
import fs from "fs";
import { Worker } from "worker_threads";


class Converter {
    constructor(directoryPath) {
        this.directoryPath = directoryPath;
    }

    start() {
        return new Promise((resolve, reject) => {
            
            fs.readdir(this.directoryPath, (err, files) => {
                if (err) {
                    console.error(err);
                    reject(err);
                    return;
                }

                const csvFiles = files.filter((file) => path.extname(file).toLowerCase() === ".csv");

                if (csvFiles.length === 0) {
                    console.error("No CSV files!");
                    reject(new Error("No CSV files!"));
                    return;
                }

                const filesCount = csvFiles.length;
                const startTime = new Date();

                const workersCount = filesCount < 10 ? filesCount : 10;
                const recordsPerWorker = Math.ceil(filesCount / workersCount);

                let completedWorkers = 0;
                let overallDuration = 0;
                let ovearallRecordCount = 0;

                for (let i = 0; i < workersCount; i++) {
                    const start = i * recordsPerWorker;
                    const end = start + recordsPerWorker;

                    const worker = new Worker("./worker.js", {
                        workerData: {
                            directoryPath: this.directoryPath,
                            csvFiles: csvFiles.slice(start, end)
                        }
                    })

                    worker.on("message", (message) => {

                        const endTime = new Date();
                        const duration = endTime - startTime;
                        overallDuration += duration;
                        ovearallRecordCount += message.recordCount;
                            
                
                        completedWorkers ++;
                
                        if (completedWorkers === workersCount) {
                            console.log(`Total record count: ${ovearallRecordCount}. Duration: ${overallDuration}ms`); 
                            resolve();
                        }
                    });
                
                    worker.on("error", (err) => {
                        console.error(err);
                        reject(err);
                    });

                    worker.on("exit", (code) => {
                        if (code !== 0) {
                            const errorMessage = `Worker stopped with exit code ${code}`;
                            console.error(errorMessage);
                            reject(new Error(errorMessage));
                        }
                    });
                }
            })    
            
        })
    }
}

const directoryPath = process.argv[2];
 console.log(directoryPath)

if (!directoryPath) {
  console.error("No such directory!");
  process.exit();
}

const converter = new Converter(directoryPath);
converter.start()
  .then(() => {
    console.log("Success!");
    process.exit();
  })
  .catch(err => {
    console.error("Failed!", err);
    process.exit();
  });

