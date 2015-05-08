{-# LANGUAGE RankNTypes #-}
module MultiCompression
    ( Compression (..)
    , compress, decompress
    , safeCopyCompress
    , safeCopyCompressMigrate
    , pCompress
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Error
import           Control.Lens
import           Control.Monad
import qualified Data.ByteString.Char8 as B
import           Data.Monoid
import qualified Data.SafeCopy         as SC
import           Data.Serialize        as Ser
import qualified Data.Streaming.Zlib   as Zlib
import           Data.Word
import           System.IO.Unsafe
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Our tag for the compression being used on the payload. We
-- carefully maintain the Serialize instance here to allow for
-- multiple storage formats.
data Compression = Snappy | Gzip | Lz4 deriving (Eq,Show,Read,Ord)


instance Serialize Compression where
    put Snappy = put (0::Word8)
    put Gzip = put (1::Word8)
    put Lz4 = put (2::Word8)

    get = do
        let g = get :: Get Word8
        i <- lookAhead g
        case i of
          0 -> g >> return Snappy
          1 -> g >> return Gzip
          2 -> g >> return Lz4
          _ -> fail "Unknown compression header."


-- | We seem to be losing only 9 bytes per object from this wrappwer.
data Compressed = Compressed {
      compressedScheme  :: Compression
    , compressedPayload :: B.ByteString
    }


instance Serialize Compressed where
    put (Compressed c i) = put c >> put i

    -- If parsing fails, get the raw bytes and assume it's legacy gzip
    get = (Compressed <$> get <*> get) <|>
          (Compressed Gzip <$> (remaining >>= getBytes))


-------------------------------------------------------------------------------
-- | Compress in a way that will support multiple compression schemes.
--
-- LZ4 is disabled as the haskell library appears to be broken ATM.
compress :: Compression -> B.ByteString -> Compressed
compress c bs = Compressed c bs'
    where
      bs' = case c of
        Gzip -> fromMaybe (error "compressGzip failed") $ compressGzip bs
        -- Snappy -> Snappy.compress bs
        -- Gzip -> toS . Gzip.compress . toS $ bs
        -- Gzip ->
          -- let m :: forall s. ST s (Either SomeException B.ByteString)
          --     m = runCatchT $ (liftM mconcat $ C.sourceList [bs] =$= C.gzip $$ C.consume)
          -- in fromMaybe (error "Could not compress to Gzip") . hush $ runST m

        -- Lz4 -> fromMaybe (error "LZ4 compression failed") $ LZ4.compress bs


-------------------------------------------------------------------------------
decompress :: Compressed -> Maybe B.ByteString
decompress (Compressed c bs) = case c of
  Gzip -> decompressGzip bs
  -- Snappy -> Just $ Snappy.decompress bs
  -- Gzip -> Just $ toS . Gzip.decompress . toS $ bs
  -- Lz4 -> LZ4.decompress bs


-- fromMaybe (error "Could not decompress from Gzip") . runST . runCatchT $
--         (mconcat $ C.sourceList [bs] =$= C.ungzip $$ C.consume)



-------------------------------------------------------------------------------
compressGzip :: B.ByteString -> Maybe B.ByteString
compressGzip bs = unsafePerformIO $ do
    i <- Zlib.initDeflate 6 (Zlib.WindowBits 31)
    p <- Zlib.feedDeflate i bs
    as <- drainPopper p
    p' <- Zlib.finishDeflate i
    let bs = hushPopper p'
    return $ do
      as' <- as
      bs' <- bs
      return $ mconcat $ as' ++ bs'



-------------------------------------------------------------------------------
decompressGzip :: B.ByteString -> Maybe B.ByteString
decompressGzip bs = unsafePerformIO $ do
    i <- Zlib.initInflate (Zlib.WindowBits 31)
    p <- Zlib.feedInflate i bs
    as <- drainPopper p
    b <- Zlib.finishInflate i
    return $ do
        as' <- as
        return $ mconcat $ as' ++ [b]


drainPopper f = go
    where
      go = do
          res <- f
          case res of
            Zlib.PRDone -> return (Just [])
            Zlib.PRError e -> return Nothing
            Zlib.PRNext bs -> do
              xs <- go
              return $ (bs:) <$> xs


hushPopper p = case p of
    Zlib.PRDone -> Just []
    Zlib.PRError e -> Nothing
    Zlib.PRNext bs -> Just [bs]



-------------------------------------------------------------------------------
-- | Compressing serialization via SafeCopy. DO NOT CHANGE this unless
-- you know exactly what you are doing. Lots of database serialization
-- elements use this.
--
-- Encoding workflow:
--
-- object     --(SafeCopy)-->
-- ByteString --(Compress)-->
-- Compressed --(Serialize)-->
-- ByteString
safeCopyCompress :: SC.SafeCopy a => Compression -> Prism' B.ByteString a
safeCopyCompress c = prism' encode decode
    where
      encode = Ser.encode . compress c . Ser.runPut . SC.safePut

      decode a = hush (Ser.decode a) >>=
                 decompress >>=
                 hush . Ser.runGet SC.safeGet


-------------------------------------------------------------------------------
-- | Migrate from uncompressed safecopy to compressed safecopy.
safeCopyCompressMigrate
    :: SC.SafeCopy a
    => Compression
    -> Prism' B.ByteString a
safeCopyCompressMigrate c = prism' encode decode
    where
      encode x = x ^. re (safeCopyCompress c)
      decode x = (x ^? safeCopyCompress c) `mplus`
                 (hush . Ser.runGet SC.safeGet) x


-------------------------------------------------------------------------------
-- | Compression using Widengle's compression scheme.
pCompress :: Compression -> Prism' B.ByteString B.ByteString
pCompress c = prism' encode decode
    where
      encode = Ser.encode . compress c
      decode a = hush (Ser.decode a) >>= decompress



