using System;

namespace BlobContainerSynchronizerConsoleTest
{
    class Program
    {
        static void Main()
        {
            try
            {
                new BlobContainerSynchronizerTest().RunTest().Wait();
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
            }
        }
    }
}
