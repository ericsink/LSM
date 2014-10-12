
using System;
using System.Collections.Generic;
using System.IO;

using Zumero.LSM;
//using Zumero.LSM.fs;

public static class hack
{
    public static string from_utf8(this Stream s)
    {
        // note the arbitrary choice of getting this function from cs instead of fs
        // maybe utils should move into LSM_base
        return Zumero.LSM.fs.utils.ReadAll (s).FromUTF8 ();
    }

    public static void Insert(this IWrite w, byte[] k, byte[] v)
    {
        w.Insert (k, new MemoryStream(v) );
    }

    public static void Insert(this IWrite w, string k, byte[] v)
    {
        w.Insert (k.ToUTF8 (), new MemoryStream(v) );
    }

    public static void Insert(this IWrite w, string k, string v)
    {
        w.Insert (k.ToUTF8 (), new MemoryStream(v.ToUTF8 ()) );
    }

    public static void Delete(this IWrite w, string k)
    {
        w.Delete (k.ToUTF8 ());
    }

    public static void Seek(this ICursor csr, string k, SeekOp sop)
    {
        csr.Seek (k.ToUTF8(), sop);
    }

    public static byte[] ToUTF8(this string s)
    {
        return System.Text.Encoding.UTF8.GetBytes (s);
    }

    public static string FromUTF8(this byte[] ba)
    {
        return System.Text.Encoding.UTF8.GetString (ba, 0, ba.Length);
    }
}

public class foo
{
	private const int PAGE_SIZE = 256;

	private static int lastPage(Stream fs)
	{
		return (int)(fs.Length / PAGE_SIZE);
	}

    public static void Main(string[] argv)
    {
    }
}

