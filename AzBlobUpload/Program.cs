using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Newtonsoft.Json;

namespace AzBlobUpload
{
    internal class Block
    {
        public string BlockId { get; set; }
        public List<string> BlockIds { get; set; }
        public int BlockNumber { get; set; }
        public int BlockSize { get; set; }
        public long RemainingBytes { get; set; }
    }

    internal class Parameters
    {
        public string ContainerName { get; set; }
        public string SourceFile { get; set; }
        public string StorageConnectionString { get; set; }
    }

    internal class Program
    {
        private static void CleanupRestartFile(string sourceFile)
        {
            if (!File.Exists(Path.Combine(Path.GetDirectoryName(sourceFile), $"{Path.GetFileNameWithoutExtension(sourceFile)}.azrestart"))) return;

            File.Delete(Path.Combine(Path.GetDirectoryName(sourceFile), $"{Path.GetFileNameWithoutExtension(sourceFile)}.azrestart"));
        }


        // {
        //      "storageConnectionString": "<storage connection string>",
        //      "containerName": "<storage container name>",
        //      "sourceFile": "<source file full path>"
        // }
        private static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Need parameters file path.");
                return;
            }

            Parameters parameters;

            try
            {
                parameters = ReadParametersFile(args[0]);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return;
            }

            ProcessAsync(parameters).GetAwaiter().GetResult();
        }

        private static async Task ProcessAsync(Parameters parameters)
        {
            if (CloudStorageAccount.TryParse(parameters.StorageConnectionString, out var storageAccount))
            {
                var restartBlock = ReadRestartFile(parameters.SourceFile);

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                try
                {
                    // Create the CloudBlobClient that represents the Blob storage endpoint for the storage account.
                    var cloudBlobClient = storageAccount.CreateCloudBlobClient();
                    var cloudBlobContainer = cloudBlobClient.GetContainerReference(parameters.ContainerName);
                    var cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(Path.GetFileName(parameters.SourceFile));

                    int blockSize = restartBlock?.BlockSize ?? 100 * (1024 * 1024); // 100MB
                    int blockNumber = restartBlock?.BlockNumber ?? 0;
                    var blockIds = restartBlock?.BlockIds ?? new List<string>();
                    long fileSize = restartBlock?.RemainingBytes ?? new FileInfo(parameters.SourceFile).Length;

                    if (restartBlock != null)
                    {
                        Console.WriteLine("!! RESTARTING UPLOAD !!");
                    }

                    Console.WriteLine($"Uploading file: {parameters.SourceFile}");
                    Console.WriteLine($"Uploading bytes: {fileSize}");
                    Console.WriteLine($"Blocks to upload: {Math.Ceiling(fileSize / (double)(100 * 1024 * 1024))}");

                    using (var fileStream = new FileStream(parameters.SourceFile, FileMode.Open, FileAccess.Read))
                    {
                        while (fileSize > 0)
                        {
                            int bufferSize = fileSize > blockSize ? blockSize : (int)fileSize;

                            var buffer = new byte[bufferSize];
                            var binaryReader = new BinaryReader(fileStream);

                            fileStream.Seek(blockSize * blockNumber, SeekOrigin.Begin);
                            binaryReader.Read(buffer, 0, bufferSize);

                            var memoryStream = new MemoryStream(buffer, 0, bufferSize);

                            string blockId = restartBlock?.BlockId ?? Convert.ToBase64String(Encoding.ASCII.GetBytes($"BlockId{blockNumber:0000000}"));
                            if (!blockIds.Contains(blockId))
                            {
                                blockIds.Add(blockId);
                            }

                            restartBlock = new Block
                            {
                                BlockId = blockId,
                                BlockIds = blockIds,
                                BlockNumber = blockNumber,
                                BlockSize = bufferSize,
                                RemainingBytes = fileSize
                            };

                            var md5 = new MD5CryptoServiceProvider();
                            var blockHash = md5.ComputeHash(buffer);
                            string md5Hash = Convert.ToBase64String(blockHash, 0, 16);

                            await cloudBlockBlob.PutBlockAsync(blockId, memoryStream, md5Hash);

                            Console.WriteLine($"Block number: {blockNumber}, Block size: {bufferSize}, Block ID: {blockId}, MD5 Hash: {md5Hash}, Blocks Written: {blockIds.Count - 1}, Elapsed time: {stopwatch.Elapsed}");

                            memoryStream.Dispose();

                            blockNumber += 1;
                            fileSize -= blockSize;
                            restartBlock = null;
                        }

                        await ReadBlockListAsync(cloudBlockBlob);
                        await cloudBlockBlob.PutBlockListAsync(blockIds);
                        await ReadBlockListAsync(cloudBlockBlob);

                        CleanupRestartFile(parameters.SourceFile);
                    }
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Error returned from the service: {0}", ex.Message);
                    WriteRestartFile(restartBlock, parameters.SourceFile);
                }
                finally
                {
                    stopwatch.Stop();
                    Console.WriteLine($"Process complete. Total elapsed time: {stopwatch.Elapsed}");
                }
            }
        }
        /// <summary>
        /// Reads the blob's block list, and indicates whether the blob has been committed.
        /// </summary>
        /// <param name="blob">A CloudBlockBlob object.</param>
        /// <returns>A Task object.</returns>
        private static async Task ReadBlockListAsync(CloudBlockBlob blob)
        {
            // Get the blob's block list.
            foreach (var listBlockItem in await blob.DownloadBlockListAsync(BlockListingFilter.All, null, null, null))
            {
                Console.WriteLine(
                    listBlockItem.Committed
                        ? "Block {0} has been committed to block list. Block length = {1}"
                        : "Block {0} is uncommitted. Block length = {1}",
                    listBlockItem.Name,
                    listBlockItem.Length);
            }

            Console.WriteLine();
        }

        private static Parameters ReadParametersFile(string parametersFile)
        {
            if (!File.Exists(parametersFile))
            {
                throw new Exception("Invalid parameters file path.");
            }

            string parametersJson = File.ReadAllText(parametersFile);
            var parameters = JsonConvert.DeserializeObject<Parameters>(parametersJson);
            return parameters;
        }
        private static Block ReadRestartFile(string sourceFile)
        {
            if (!File.Exists(Path.Combine(Path.GetDirectoryName(sourceFile), $"{Path.GetFileNameWithoutExtension(sourceFile)}.azrestart"))) return null;

            var restartRaw = File.ReadAllBytes(Path.Combine(Path.GetDirectoryName(sourceFile), $"{Path.GetFileNameWithoutExtension(sourceFile)}.azrestart"));
            string restartString = Encoding.ASCII.GetString(restartRaw);
            var restartBlock = JsonConvert.DeserializeObject<Block>(restartString);
            return restartBlock;
        }

        private static void WriteRestartFile(Block restartBlock, string sourceFile)
        {
            string startBlockDump = JsonConvert.SerializeObject(restartBlock);
            File.WriteAllBytes(Path.Combine(Path.GetDirectoryName(sourceFile), $"{Path.GetFileNameWithoutExtension(sourceFile)}.azrestart"), Encoding.ASCII.GetBytes(startBlockDump));
        }
    }
}
