package run.ikaros.plugin.file;

import org.pf4j.PluginWrapper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import run.ikaros.api.constant.FileConst;
import run.ikaros.api.core.file.FileOperate;
import run.ikaros.api.core.file.Folder;
import run.ikaros.api.core.file.FolderOperate;
import run.ikaros.api.infra.properties.IkarosProperties;
import run.ikaros.api.infra.utils.FileUtils;
import run.ikaros.api.plugin.BasePlugin;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LocalFilesImportPlugin extends BasePlugin {
    private static final String NAME = "PluginLocalFilesImport";
    private final IkarosProperties ikarosProperties;
    private final FileOperate fileOperate;
    private final FolderOperate folderOperate;

    public LocalFilesImportPlugin(PluginWrapper wrapper, IkarosProperties ikarosProperties,
                                  FileOperate fileOperate, FolderOperate folderOperate) {
        super(wrapper);
        this.ikarosProperties = ikarosProperties;
        this.fileOperate = fileOperate;
        this.folderOperate = folderOperate;
    }


    @Override
    public void start() {
        log.info("plugin [{}] start success", NAME);
        linkFiles();
    }

    @Override
    public void stop() {
        log.info("plugin [{}] stop success", NAME);
    }

    @Override
    public void delete() {
        log.debug("plugin [{}] delete success", NAME);
    }


    @Async
    protected void linkFiles() {
        log.info("start import links dir files.");
        long start = System.currentTimeMillis();
        Path links = ikarosProperties.getWorkDir().resolve("links");
        File linksDir = links.toFile();
        if (linksDir.isFile()) {
            throw new LocalFilesImportException("current links must dir.");
        }
        if (!linksDir.exists()) {
            linksDir.mkdirs();
            log.info("mkdir links dir, path={}", linksDir);
        }

        handleImportDirFile(linksDir, FileConst.DEFAULT_FOLDER_ROOT_ID)
            .subscribeOn(Schedulers.parallel())
            .subscribe();


        log.info("end import links dir files, time: {}", System.currentTimeMillis() - start);
    }

    private run.ikaros.api.core.file.File handleSingle(File file, Long parentId, String md5)
        throws IOException {
        // 创建上传文件目录
        String name = file.getName();
        String filePostfix = FileUtils.parseFilePostfix(name);
        String uploadFilePath
            = FileUtils.buildAppUploadFilePath(
            ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
            filePostfix);
        File uploadFile = new File(uploadFilePath);


        try {
            Files.createLink(uploadFile.toPath(), file.toPath());
            log.info("copy file hard link from: {}, to: {}",
                file.getAbsolutePath(), uploadFile.getAbsolutePath());
        } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            log.warn("file already exists for path: [{}].", uploadFile.getAbsolutePath());
            return null;
        } catch (FileSystemException fileSystemException) {
            // 硬链接失败则进行复制
            log.warn("file hard links fail from: {}, to: {}",
                file.getAbsolutePath(), uploadFile.getAbsolutePath());
            Files.copy(file.toPath(), uploadFile.toPath());
            log.info("copy file from: {}, to: {}",
                file.getAbsolutePath(), uploadFile.getAbsolutePath());
        }

        run.ikaros.api.core.file.File fileDto = new run.ikaros.api.core.file.File();
        fileDto.setFolderId(parentId);
        fileDto.setName(name);
        fileDto.setType(FileUtils.parseTypeByPostfix(filePostfix));
        fileDto.setMd5(md5);
        fileDto.setUrl(
            uploadFilePath.replace(ikarosProperties.getWorkDir().toFile().getAbsolutePath(), "")
                .replace("\\", "/"));
        fileDto.setCanRead(true);
        fileDto.setType(FileUtils.parseTypeByPostfix(filePostfix));
        fileDto.setFsPath(uploadFile.getAbsolutePath());
        fileDto.setSize(file.length());
        fileDto.setUpdateTime(LocalDateTime.now());
        return fileDto;
    }


    private Mono<Void> handleImportDirFile(File file, Long parentId) {
        if (file.isDirectory()) {
            // ignore . and .. create dir.
            if (".".equalsIgnoreCase(file.getName()) || "..".equalsIgnoreCase(file.getName())) {
                return Mono.empty();
            }
            // dir
            return folderOperate.create(parentId, file.getName())
                .map(Folder::getId)
                .flatMapMany(id -> Flux.fromStream(Arrays.stream(
                        Objects.requireNonNull(file.listFiles())))
                    .flatMap(file1 -> handleImportDirFile(file1, id)))
                .then();
        } else {
            // file
            if (!file.exists()) {
                return Mono.empty();
            }


            String md5 = "";
            try {
                md5 = FileUtils.calculateFileHash(FileUtils.convertToDataBufferFlux(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Assert.hasText(md5, "'md5' must has text.");

            String finalMd = md5;
            return fileOperate.existsByMd5(md5)
                .filter(exist -> !exist)
                .subscribeOn(Schedulers.parallel())
                .flatMap(exists -> {
                    try {
                        return fileOperate.create(handleSingle(file, parentId, finalMd)).then();
                    } catch (IOException e) {
                        return Mono.error(new RuntimeException(e));
                    }
                });

        }
    }
}
