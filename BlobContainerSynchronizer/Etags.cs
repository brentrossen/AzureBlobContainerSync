using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BlobContainerSynchronizer
{
    /// <summary>
    /// Represents a set of etags and facilitates checking for new etags
    /// and saving the etags to disk.
    /// </summary>
    internal class Etags
    {
        /// <summary>
        /// Dictionary containing the mapping of entity name to etag.
        /// It is a concurrent dictionary because IsNewEtag may be called in parallel,
        /// which will cause parallel updates to the dictionary.
        /// </summary>
        private readonly ConcurrentDictionary<string, string> etagsDictionary =
            new ConcurrentDictionary<string, string>();

        private Etags() { }

        private Etags(ConcurrentDictionary<string, string> etagsDictionary)
        {
            this.etagsDictionary = etagsDictionary;
        }

        /// <summary>
        /// Reads the etags file from disk
        /// </summary>
        public static async Task<Etags> ReadEtagsAsync(string filePath)
        {
            if (filePath == null) throw new ArgumentNullException("filePath");

            if (!File.Exists(filePath)) return new Etags();

            // deserialize from disk
            using (var etagFileStream = File.Open(filePath, FileMode.OpenOrCreate))
            {
                using (var fileReader = new StreamReader(etagFileStream))
                {
                    string dictionaryJson = await fileReader.ReadToEndAsync();
                    var dictionary = JsonConvert.DeserializeObject<ConcurrentDictionary<string, string>>(dictionaryJson);
                    return new Etags(dictionary);
                }
            }
        }

        /// <summary>
        /// Saves the etags file back to disk. This should be done after every update
        /// in case the VM goes down.
        /// </summary>
        public async Task SaveEtagsAsync(string filePath)
        {
            if (filePath == null) throw new ArgumentNullException("filePath");

            using (var etagFileStream = File.Open(filePath, FileMode.OpenOrCreate))
            {
                using (var fileWriter = new StreamWriter(etagFileStream))
                {
                    string dictionaryJson = JsonConvert.SerializeObject(etagsDictionary);

                    await fileWriter.WriteAsync(dictionaryJson);
                }
            }
        }

        /// <summary>
        /// Checks if the etag is new or old.
        /// If it is a new etag, the etag in the file will be updated.
        /// All entity names in the etags set should be considered unique.
        /// <remarks>This method is thread safe.</remarks>
        /// </summary>
        /// <param name="entityName">The name of the entity (such as blob name)</param>
        /// <param name="etag">The etag to check</param>
        /// <returns>Returns true if the etag is either not in the list or is a new etag.</returns>
        public bool IsNewEtagAndUpdate(string entityName, string etag)
        {
            if (entityName == null) throw new ArgumentNullException("entityName");
            if (etag == null) throw new ArgumentNullException("etag");

            string value;
            if (etagsDictionary.TryGetValue(entityName, out value) && value == etag) return false;

            etagsDictionary[entityName] = etag;
            return true;
        }
    }
}